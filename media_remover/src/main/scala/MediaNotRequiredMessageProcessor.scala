import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.implicits.toTraverseOps
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, MessageProcessorConverters, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.{AssetSweeperNewFile, MultiProjectOnlineOutputMessage}
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import io.circe.generic.auto._
import io.circe.syntax._
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.{LoggerFactory, MDC}

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class MediaNotRequiredMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  nearlineRecordDAO: NearlineRecordDAO,
  failureRecordDAO: FailureRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
  vidispineCommunicator: VidispineCommunicator,
  fileCopier: FileCopier,
  uploader: FileUploader
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  def copyFile(
      vault: Vault,
      file: AssetSweeperNewFile,
      maybeNearlineRecord: Option[NearlineRecord]
  ): Future[Either[String, Json]] = {
    val fullPath = Paths.get(file.filepath, file.filename)

    fileCopier
      .copyFileToMatrixStore(vault, file.filename, fullPath)
      .flatMap({
        case Right(objectId) =>
          val record = maybeNearlineRecord match {
            case Some(rec) => rec
            case None =>
              NearlineRecord(objectId, fullPath.toString, newCorrelationId)
          }

          MDC.put("correlationId", record.correlationId)

          nearlineRecordDAO
            .writeRecord(record)
            .map(recId =>
              Right(
                record
                  .copy(
                    id = Some(recId),
                    originalFilePath = fullPath.toString,
                    expectingVidispineId = !file.ignore
                  )
                  .asJson
              )
            )
        case Left(error) => Future(Left(error))
      })
      .recoverWith({
        case err: BailOutException =>
          logger.warn(
            s"A permanent exception occurred when trying to copy $fullPath: ${err.getMessage}"
          )
          Future.failed(err)
        case err: Throwable =>
          val attemptCount = attemptCountFromMDC() match {
            case Some(count) => count
            case None =>
              logger.warn(
                s"Could not get attempt count from logging context for $fullPath, creating failure report with attempt 1"
              )
              1
          }

          val rec = FailureRecord(
            id = None,
            originalFilePath = fullPath.toString,
            attempt = attemptCount,
            errorMessage = err.getMessage,
            errorComponent = ErrorComponents.Internal,
            retryState = RetryStates.WillRetry
          )
          failureRecordDAO.writeRecord(rec).map(_ => Left(err.getMessage))
      })
  }

  /** performs a search on the given vault looking for a matching file (i.e. one with the same file name AND size).
    * If one exists, it will create a NearlineRecord linking that file to the given name, save it to the database and return it.
    * Intended for use if no record exists already but a file may exist on the storage.
    *
    * @param vault vault to check
    * @param file  AssetSweeperNewFile record
    * @return a Future, containing the newly saved NearlineRecord if a record is found or None if not.
    */
  protected def checkForPreExistingFiles(
      vault: Vault,
      file: AssetSweeperNewFile
  ) = {
    val filePath = Paths.get(file.filepath, file.filename)

    for {
      matchingNearlineFiles <- fileCopier.findMatchingFilesOnNearline(
        vault,
        filePath,
        file.size
      )
      maybeVidispineMatches <- vidispineCommunicator
        .searchByPath(filePath.toString)
        .recover({ case err: Throwable => //don't abort the process if VS is not playing nice
          logger.error(
            s"Could not consult Vidispine for new file $filePath: ${err.getMessage}"
          )
          None
        }) //check if we have something in Vidispine too
      result <-
        if (matchingNearlineFiles.isEmpty) {
          logger.info(s"Found no pre-existing archived files for $filePath")

          Future(None)
        } else {
          logger.info(
            s"Found ${matchingNearlineFiles.length} archived files for $filePath: ${matchingNearlineFiles.map(_.pathOrFilename).mkString(",")}"
          )
          val newRec = NearlineRecord(
            objectId = matchingNearlineFiles.head.oid,
            originalFilePath = filePath.toString,
            correlationId = newCorrelationId
          )

          val updatedRec = maybeVidispineMatches
            .flatMap(_.file.headOption)
            .flatMap(_.item.map(_.id)) match {
            case Some(vidispineId) =>
              newRec.copy(vidispineItemId = Some(vidispineId))
            case None => newRec
          }
          nearlineRecordDAO
            .writeRecord(updatedRec)
            .map(newId => Some(updatedRec.copy(id = Some(newId))))
        }
    } yield result
  }

  def processFile(
      file: AssetSweeperNewFile,
      vault: Vault
  ): Future[Either[String, Json]] = {
    val fullPath = Paths.get(file.filepath, file.filename)

    for {
      maybeNearlineRecord <- nearlineRecordDAO.findBySourceFilename(
        fullPath.toString
      ) //check if we have a record of this file in the database
      maybePreExistingRecord <-
        if (maybeNearlineRecord.isEmpty) checkForPreExistingFiles(vault, file)
        else Future(None) //if not then check the appliance itself
      result <-
        if (maybePreExistingRecord.isDefined)
          Future(Right(maybePreExistingRecord.asJson))
        else copyFile(vault, file, maybeNearlineRecord)
    } yield result
  }

  override def handleMessage(
      routingKey: String,
      msg: Json,
      framework: MessageProcessingFramework
  ): Future[Either[String, MessageProcessorReturnValue]] = {
    (msg.as[MultiProjectOnlineOutputMessage], routingKey) match {
      case (Left(err), _) =>
        Future.failed(
          new RuntimeException(
            s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"
          )
        )

      case (
            Right(nearlineNotRequired),
            "storagetier.restorer.media_not_required.nearline"
          ) =>
        handleNearline(nearlineNotRequired)

      case (
            Right(onlineNotRequired),
            "storagetier.restorer.media_not_required.online"
          ) =>
        handleOnline(onlineNotRequired)

      case (_, _) =>
        logger.warn(
          s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it. This should be fixed in the code."
        )
        Future.failed(
          new RuntimeException(
            s"Routing key $routingKey dropped because I don't know how to handle it"
          )
        )
    }
  }

//  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] = {
//    if (!routingKey.endsWith("new") && !routingKey.endsWith("update")) return Future.failed(SilentDropMessage())
//
//    msg.as[AssetSweeperNewFile] match {
//      case Left(err) =>
//        Future(Left(s"Could not parse incoming message: $err"))
//      case Right(file) =>
//        val fullPath = Paths.get(file.filepath, file.filename)
//        if (!Files.exists(fullPath) || !Files.isRegularFile(fullPath)) {
//          logger.error(s"File $fullPath does not exist, or it's not a regular file. Can't continue.")
//          Future.failed(SilentDropMessage(Some(s"Invalid file $fullPath")))
//        } else {
//          matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
//            processFile(file, vault)
//          }
//        }
//    }
//  }

  def _existsInDeepArchive(onlineOutputMessage: MultiProjectOnlineOutputMessage): Boolean = {
    val fileSize = 0 // TODO should come from onlineOutputMessage??
    val checksum = None // TODO should come from onlineOutputMessage??
    val objectKey = onlineOutputMessage.filePath.getOrElse("nopath")
    uploader.objectExistsWithSizeAndOptionalChecksum(objectKey, fileSize, checksum) match {
      case Success(true)=>
        logger.info(s"File with objectKey $objectKey and size $fileSize exists, safe to delete from higher level")
        true
      case Success(false)=>
        logger.info(s"No file $objectKey with matching size $fileSize found, do not delete")
        false
      case Failure(err)=>
        logger.warn(s"Could not connect to deep archive to check if media exists, do not delete. Err: $err")
        false
    }
  }

  def _removeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) :Either[String, String] = ???

  def _deleteFromNearline(onlineOutputMessage: MultiProjectOnlineOutputMessage): Either[String, MediaRemovedMessage] = ???

  def _storeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage): Either[String, Boolean] = ???

  def _outputDeepArchiveCopyRequried(onlineOutputMessage: MultiProjectOnlineOutputMessage): Either[String, NearlineRecord] = ???

  def _existsInInternalArchive(onlineOutputMessage: MultiProjectOnlineOutputMessage): Boolean = ???

  def _outputInternalArchiveCopyRequried(onlineOutputMessage: MultiProjectOnlineOutputMessage): Either[String, NearlineRecord] = ???

  def processNearlineFileAndProject(onlineOutputMessage: MultiProjectOnlineOutputMessage, maybeProject: Option[ProjectRecord]):Future[Either[String, MessageProcessorReturnValue]] = {
//  def processNearlineFileAndProject(onlineOutputMessage: MultiProjectOnlineOutputMessage, maybeProject: Option[ProjectRecord]) = {
    import MediaRemovedMessage._
    val ignoreReason = maybeProject match {
      case None =>
        val noProjectFoundMsg = s"No project could be found that is associated with $onlineOutputMessage, erring on the safe side, not removing"
        logger.warn(noProjectFoundMsg)
        throw SilentDropMessage(Some(noProjectFoundMsg))

      case Some(project) =>
        if (project.deletable.getOrElse(false)) {
          logger.debug(s"-> project ${project.id.getOrElse(-1)} IS DELETABLE")

          if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
            // project is COMPLETED or KILLED
            logger.debug(s"-> project ${project.id.getOrElse(-1)} is ${EntryStatus.Completed} or ${EntryStatus.Killed}")

              if (onlineOutputMessage.mediaCategory.toLowerCase.equals("deliverables")) {
                // media is DELIVERABLES
                val notRemovingMsg = s"not removing file ${onlineOutputMessage.itemId}, project ${project.id.getOrElse(-1)} has status ${project.status} and is deletable, BUT the media is a deliverable"
                logger.debug(s"-> $notRemovingMsg")
                throw SilentDropMessage(Some(notRemovingMsg))
              } else {
                // media is NOT DELIVERABLES
                // TODO Remove "pending deletion" record if exists
                _removeDeletionPending(onlineOutputMessage)
                // TODO Delete media from storage
                _deleteFromNearline(onlineOutputMessage) match {
                  case Left(err) => Left(err)
                  case Right(msg) =>
                    logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                    Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
                }
              }

          }
          else {
            // project is neither COMPLETED nor KILLED
            val notRemovingMsg = s"not removing file ${onlineOutputMessage.itemId}, project ${project.id.getOrElse(-1)} is deletable, BUT is status is neither ${EntryStatus.Completed} nor ${EntryStatus.Killed}; it is ${project.status}"
            logger.debug(s"-> $notRemovingMsg")
            throw SilentDropMessage(Some(notRemovingMsg))
          }

        } else {
          // not DELETABLE
          logger.debug(s"-> project ${project.id.getOrElse(-1)} IS NOT DELETABLE")

          if (project.deep_archive.getOrElse(false)) {
            // project is DEEP ARCHIVE
            logger.debug(s"--> project ${project.id.getOrElse(-1)} IS DEEP_ARCHIVE")

            if (project.sensitive.getOrElse(false)) {
              // project is SENSITIVE
              logger.debug(s"--> project ${project.id.getOrElse(-1)} IS SENSITIVE")

              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                // project is COMPLETED or KILLED
                logger.debug(s"--> project ${project.id.getOrElse(-1)} is ${EntryStatus.Completed} or ${EntryStatus.Killed}")

                if (_existsInInternalArchive(onlineOutputMessage)) {
                  // media EXISTS in INTERNAL ARCHIVE
                  _removeDeletionPending(onlineOutputMessage)
                  _deleteFromNearline(onlineOutputMessage) match {
                    case Left(err) => Left(err)
                    case Right(msg) =>
                      logger.debug(s"--> deleting ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                      Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
                  }
                } else {
                  // media does NOT EXIST in INTERNAL ARCHIVE
                  _storeDeletionPending(onlineOutputMessage)
                  _outputInternalArchiveCopyRequried(onlineOutputMessage) match {
                    case Left(err) => Left(err)
                    case Right(msg) =>
                      logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                      Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
                  }
                }

              }
              else {
                val notRemovingMsg = s"not removing file ${onlineOutputMessage.itemId}, project ${project.id.getOrElse(-1)} is deletable, BUT is status is neither ${EntryStatus.Completed} nor ${EntryStatus.Killed}; it is ${project.status}"
                logger.debug(s"-> $notRemovingMsg")
                throw SilentDropMessage(Some(notRemovingMsg))
              }

            } else {
              logger.debug(s"--> project ${project.id.getOrElse(-1)} IS NOT SENSITIVE")

              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                logger.debug(s"--> project ${project.id.getOrElse(-1)} is ${EntryStatus.Completed} or ${EntryStatus.Killed}")

                if (_existsInDeepArchive(onlineOutputMessage)) {
                  _removeDeletionPending(onlineOutputMessage)
                  _deleteFromNearline(onlineOutputMessage) match {
                    case Left(err) => Left(err)
                    case Right(msg) =>
                      logger.debug(s"--> deleting ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                      Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
                  }
                } else {
                  _storeDeletionPending(onlineOutputMessage)
                  _outputDeepArchiveCopyRequried(onlineOutputMessage) match {
                    case Left(err) => Left(err)
                    case Right(msg) =>
                      logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                      Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
                  }
                }

              }
              else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                // project is neither COMPLETED nor KILLED
                val notRemovingMsg = s"not removing nearline media ${onlineOutputMessage.nearlineId}, project ${project.id.getOrElse(-1)} is deep_archive and not sensitive, BUT is status is neither ${EntryStatus.Completed} nor ${EntryStatus.Killed}; it is ${project.status}"
                logger.debug(s"-> $notRemovingMsg")
                throw SilentDropMessage(Some(notRemovingMsg))
              }
            }

          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
            throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
          }
        }
    }
//    logger.debug(s"---> ignoreReason: $ignoreReason")
    Future(ignoreReason)
  }

  def bulkGetProjectMetadata(
      mediaNotRequiredMsg: MultiProjectOnlineOutputMessage
  ) = {
    for {
      result <- {
        val ids = mediaNotRequiredMsg.projectIds
        val projectMds =
          ids.map(projectId => asLookup.getProjectMetadata(projectId.toString))
        projectMds.sequence
//        val failures = projectMds.collect({ case Failure(err) => err })
//        if(failures.nonEmpty){
//          logger.error(s"${failures.length} messages failed to send to $output_exchange_name.")
//          failures.foreach(err => {
//            logger.error(s"\t${err.getMessage}")
//            logger.debug(s"${err.getMessage}", err)
//          })
//          Failure(new RuntimeException(s"${failures.length} / ${msgs.length} messages failed to send to $output_exchange_name as $routingKey"))
//        } else {
//          Success()
//        }
//      }
      }
    } yield result

  }

  private def handleNearline(
      mediaNotRequiredMsg: MultiProjectOnlineOutputMessage
  ): Future[Either[String, MessageProcessorReturnValue]] = {

    val mds = bulkGetProjectMetadata(mediaNotRequiredMsg)
    val mds2: Future[Seq[ProjectRecord]] = mds.map(_.map(_.get))
//      .map(_.head)
//      .collect({
//        case x if x. == EntryStatus.New => x
////        case x if x.head.status == EntryStatus.New => x
//      })
//    mds2.foreach(st => logger.debug(st))
//    mds2.foreach(st => logger.debug(s"nah bruh ${st.head.id.getOrElse(-1)}: ${st.head.status}"))
//    mds2.map(_.foreach(st => logger.debug(s"ooowee ${st.id.getOrElse(-1)}: ${st.status}")))
    val mds3: Future[Seq[ProjectRecord]] = mds2.map(_.collect({
      case x if x.status == EntryStatus.InProduction => x
    }))
//    mds3.map(_.foreach(st => logger.debug(s"filturrd ${st.id.getOrElse(-1)}: ${st.status}")))

    // check if still removable project status
    val removeResultFut = for {
      projectRecord <- mds2.map(_.headOption)
//      projectRecord <- asLookup.getProjectMetadata(mediaNotRequiredMsg.projectIds.head.toString)
      fileRemoveResult <- processNearlineFileAndProject(
        mediaNotRequiredMsg,
        projectRecord
      )
    } yield fileRemoveResult


//    removeResultFut.map(r => logger.debug(s"rrrrrrrrrr: $r"))
    removeResultFut

/*
     handle seq of projects
     if any is new or in production, then silentdrop
     do we need to handle differing statuses other than that, say if differeing in sensitivity?
     may we have to send out deepArchiveRequest AND internalArchiveRequest for the same file??

     if at least one sensitive, check if on internalArchive
     if at least one deep_archive, check if on deepArchive
*/


//    val projectMdSingle = for {
//      maybeProject <- asLookup.getProjectMetadata(mediaNotRequiredMsg.projectId.toString)
//    } yield maybeProject.map(_.status).get
//
//    projectMdSingle.map(da => {
//      val projStatus = da
//      projStatus match {
//        case EntryStatus.New => logger.debug(s"aaa2: ${projStatus}")
//        case EntryStatus.Held => logger.debug(s"bbb2: ${projStatus}")
//        case EntryStatus.Completed => logger.debug(s"ccc2: ${projStatus}")
//        case EntryStatus.Killed => logger.debug(s"ddd2: ${projStatus}")
//        case EntryStatus.InProduction => logger.debug(s"eee2: ${projStatus}")
//        case _ => logger.debug("wut?")
//      }
//    })
  }

  private def handleOnline(
      mediaNotRequiredMsg: MultiProjectOnlineOutputMessage
  ) = {
//    logger.debug(s"Online $mediaNotRequiredMsg")
    if (mediaNotRequiredMsg.projectIds.head == 22)
      Future(Left("testing online"))
    else
      Future(Right(mediaNotRequiredMsg.asJson))
  }
}
