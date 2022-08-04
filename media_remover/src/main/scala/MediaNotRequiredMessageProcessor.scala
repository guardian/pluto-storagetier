import MediaNotRequiredMessageProcessor.Action
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import cats.implicits.toTraverseOps
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.helpers.{MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper.getOMFileMd5
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.streamcomponents.ChecksumSink
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.{LoggerFactory, MDC}

import java.util.{Map, UUID}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class MediaNotRequiredMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
  vidispineCommunicator: VidispineCommunicator,
  s3ObjectChecker: S3ObjectChecker
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] = {
    routingKey match {
      case "storagetier.restorer.media_not_required.nearline" =>
        msg.as[OnlineOutputMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"))
          case Right(onlineOutputMessageNearline) =>
            // TODO? Move the withVaults inside handleNearline?
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleNearline(nearlineVault, internalArchiveVault, onlineOutputMessageNearline)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }
      case "storagetier.restorer.media_not_required.online" =>
        msg.as[OnlineOutputMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"))
          case Right(onlineOutputMessageOnline) =>
            handleOnline(onlineOutputMessageOnline)
        }
      case _ =>
        logger.warn(s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it.")
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }
  }




  //------------------------------------------------------------------


  protected def openMxsObject(vault:Vault, oid:String) = Try { vault.getObject(oid) }

  /**
   * If the given file already exists in the MXS vault (i.e., there is a file with a matching MXFS_PATH _and_ checksum
   * _and_ file size, then a Right ..
   * If there is a file with matching MXFS_PATH but checksum and/or size do not match, then ...
   *
   * @param vault    Vault to check
   * @param fileName file name to check on the Vault
   * @param filePath the filepath of the item to check
   * */
  //  def copyFileToMatrixStore(vault: Vault, fileName: String, filePath: Path): Future[Either[String, String]] = {
  def existsInTargetVaultWithMd5Match(vault: Vault, fileName: String, filePath: String, fileSize: Long, maybeLocalChecksum: Option[String]): Future[Boolean] = {
    for {
      potentialMatches <- findMatchingFilesOnVault(vault, filePath, fileSize)
      potentialMatchesFiles <- Future.sequence(potentialMatches.map(entry => Future.fromTry(openMxsObject(vault, entry.oid))))
      alreadyExists <- verifyChecksumMatch(filePath, potentialMatchesFiles, maybeLocalChecksum)
      result <- alreadyExists match {
        case Some(existingId) =>
          logger.info(s"$filePath: Object already exists with object id $existingId")
          Future(true)
        case None =>
          logger.info(s"$filePath: Out of ${potentialMatches.length} remote matches, none matched the checksum")
          Future(false)
      }
    } yield result
  }

  protected def getContextMap() = {
    Option(MDC.getCopyOfContextMap)
  }

  protected def setContextMap(contextMap: Map[String, String]) = {
    MDC.setContextMap(contextMap)
  }

  protected def getOMFileMd5(mxsFile: MxsObject) = {
    MatrixStoreHelper.getOMFileMd5(mxsFile)
  }

  protected def getSizeFromMxs(mxsFile: MxsObject) = {
    MetadataHelper.getFileSize(mxsFile)
  }


  /**
   * Checks to see if any of the MXS files in the `potentialFiles` list are a checksum match for `filePath`.
   * Stops and returns the ID of the first match if it finds one, or returns None if there were no matches.
   *
   * @param filePath           local file that is being backed up
   * @param potentialFiles     potential backup copies of this file
   * @param maybeLocalChecksum stored local checksum; if set this is used instead of re-calculating. Leave this out when calling.
   * @return a Future containing the OID of the first matching file if present or None otherwise
   */
  //  protected def verifyChecksumMatch(filePath:Path, potentialFiles:Seq[MxsObject], maybeLocalChecksum:Option[String]=None):Future[Option[String]] = potentialFiles.headOption match {
  protected def verifyChecksumMatch(filePath: String, potentialFiles: Seq[MxsObject], maybeLocalChecksum: Option[String]): Future[Option[String]] =
 {
   val verifiedMaybeLocalChecksumFut = maybeLocalChecksum match {
     case None => throw new RuntimeException("Must be called with Some(actualChecksum)")
     case Some(value) => Future(Some(value))
   }
    potentialFiles.headOption match {
      case None =>
        logger.info(s"$filePath: No matches found for file checksum")
        Future(None)
      case Some(mxsFileToCheck) =>
        logger.info(s"$filePath: Verifying checksum for MXS file ${mxsFileToCheck.getId}")
        val savedContext = getContextMap() //need to save the debug context for when we go in and out of akka
        val requiredChecksums = Seq(
          verifiedMaybeLocalChecksumFut,
          getOMFileMd5(mxsFileToCheck)
        )
        Future
          .sequence(requiredChecksums)
          .map(results => {
            if (savedContext.isDefined) setContextMap(savedContext.get)
            val localChecksum = results.head.asInstanceOf[Option[String]]
            val applianceChecksum = results(1).asInstanceOf[Try[String]]
            logger.info(s"$filePath: local checksum is $localChecksum, ${mxsFileToCheck.getId} checksum is $applianceChecksum")
            (localChecksum == applianceChecksum.toOption, localChecksum)
          })
          .flatMap({
            case (true, _) =>
              logger.info(s"$filePath: Got a checksum match for remote file ${mxsFileToCheck.getId}")
              Future(Some(potentialFiles.head.getId)) //true => we got a match
            case (false, localChecksum) =>
              logger.info(s"$filePath: ${mxsFileToCheck.getId} did not match, trying the next entry of ${potentialFiles.tail.length}")
              verifyChecksumMatch(filePath, potentialFiles.tail, localChecksum)
          })
    }
}

  protected def callFindByFilenameNew(vault:Vault, fileName:String) = MatrixStoreHelper.findByFilenameNew(vault, fileName)
  protected def callObjectMatrixEntryFromOID(vault:Vault, fileName:String) = ObjectMatrixEntry.fromOID(fileName, vault)

  /**
   * Searches the given vault for files matching the given specification.
   * Both the name and fileSize must match in order to be considered valid.
   * @param vault vault to search
   * @param filePath Path representing the file path to look for.
   * @param fileSize Long representing the size of the file to match
   * @return a Future, containing a sequence of ObjectMatrixEntries that match the given file path and size
   */
//  def findMatchingFilesOnNearline_fromFileCopier(vault: Vault, filePath: Path, fileSize: Long) = {
  def findMatchingFilesOnVault(vault: Vault, filePath: String, fileSize: Long) = {
    logger.debug(s"Looking for files matching $filePath at size $fileSize")
    callFindByFilenameNew(vault, filePath)
      .map(fileNameMatches=>{
        val nullSizes = fileNameMatches.map(_.maybeGetSize()).collect({case None=>None}).length
        // TODO Decide if we need to/should do this? Flip it around maybe?
        if(nullSizes>0) {
          throw new BailOutExceptionMR(s"Could not check for matching files of $filePath because $nullSizes / ${fileNameMatches.length} had no size")
        }

        val sizeMatches = fileNameMatches.filter(_.maybeGetSize().contains(fileSize))
        logger.debug(s"$filePath: ${fileNameMatches.length} files matched name and ${sizeMatches.length} matched size")
        logger.debug(fileNameMatches.map(obj=>s"${obj.pathOrFilename.getOrElse("-")}: ${obj.maybeGetSize()}").mkString("; "))
        sizeMatches
      })
  }

  def getChecksumForNearlineItem(vault: Vault, oid: String): Future[Option[String]] = {
    for {
      mxsFile <- Future.fromTry(openMxsObject(vault, oid))
      maybeMd5 <- MatrixStoreHelper.getOMFileMd5(mxsFile).flatMap({
            case Failure(err) =>
              logger.error(s"Unable to get checksum from appliance, file should be considered unsafe", err)
              Future(None)
            case Success(remoteChecksum) =>
              logger.info(s"Appliance reported checksum of $remoteChecksum")
              Future(Some(remoteChecksum))
          })
      } yield maybeMd5
  }

  def existsInInternalArchive(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, fileName, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.filePath, onlineOutputMessage.nearlineId)
    for {
      maybeChecksum <- getChecksumForNearlineItem(vault, nearlineId)
      exists <- existsInTargetVaultWithMd5Match(internalArchiveVault, fileName, fileName, fileSize, maybeChecksum)
    } yield exists
  }

  def existsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, objectKey, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.filePath, onlineOutputMessage.nearlineId)
    val maybeChecksumFut = getChecksumForNearlineItem(vault, nearlineId)
    maybeChecksumFut.map(maybeChecksum =>
      s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(objectKey, fileSize, maybeChecksum) match {
        case Success(true) =>
          logger.info(s"File with objectKey $objectKey and size $fileSize exists, safe to delete from higher level")
          true
        case Success(false) =>
          logger.info(s"No file $objectKey with matching size $fileSize found, do not delete")
          false
        case Failure(err) =>
          logger.warn(s"Could not connect to deep archive to check if media exists, do not delete. Err: $err")
          false
      }
    )
  }

  def removeDeletionPending(existingRecord: PendingDeletionRecord): Future[Int] =
    pendingDeletionRecordDAO.deleteRecord(existingRecord)

  def removeDeletionPendingByMessage(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    (msg.mediaTier, msg.itemId, msg.nearlineId) match {
      case ("NEARLINE", _, Some(nearlineId)) =>
        pendingDeletionRecordDAO
          .findByNearlineId(nearlineId)
          .flatMap({
            case Some(existingRecord)=>
              logger.debug(s"Deleting pendingDeletionRecord ${existingRecord.id.getOrElse(-1)} for ${msg.mediaTier}, oid ${msg.nearlineId}")
              pendingDeletionRecordDAO.deleteRecord(existingRecord).map(i => Right(i))
            case None=>
              Future(Left("Should not happen"))
          })
      case ("NEARLINE", _, _) =>
        Future.failed(new RuntimeException("NEARLINE but no nearlineId"))

      case ("ONLINE", Some(onlineId), _) =>
        logger.warn(s"Not implemented yet - $onlineId ignored")
        Future.failed(SilentDropMessage(Some(s"Not implemented yet - $onlineId ignored")))
      case ("ONLINE", _, _) =>
        Future.failed(new RuntimeException("ONLINE but no onlineId"))

      case (_, _, _) =>
        Future.failed(new RuntimeException("This should not happen!"))
    }

  def deleteSingleAttMedia(vault: Vault, objectMatrixEntry: ObjectMatrixEntry, attributeKey: String, msg: OnlineOutputMessage): Future[Either[String, Boolean]] =
    objectMatrixEntry.stringAttribute(attributeKey) match {
      case None =>
        logger.info(s"No $attributeKey to remove for main nearline media oid=${msg.nearlineId}, path=${msg.filePath}")
        Future(Right(false))
      case Some(attOid) =>
        Try { vault.getObject(attOid).delete() } match {
          case Success(_) =>
            logger.info(s"$attributeKey with oid $attOid removed for main nearline media oid=${msg.nearlineId}, path=${msg.filePath}")
            Future(Right(true))
          case Failure(exception) =>
            logger.warn(s"Failed to remove nearline media oid=${msg.nearlineId}, path=${msg.filePath}, reason: ${exception.getMessage}")
            Future(Left(s"Failed to remove $attributeKey with oid=$attOid for main nearline media oid=${msg.nearlineId}, path=${msg.filePath}, reason: ${exception.getMessage}"))
        }
    }

  def dealWithAttFiles(vault: Vault, nearlineId: String, msg: OnlineOutputMessage) = {
    val combinedRes = for {
      objectMatrixEntry <- callObjectMatrixEntryFromOID(vault, nearlineId)
      proxyRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_PROXY_OID", msg)
      thumbRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_THUMB_OID", msg)
      metaRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_META_OID", msg)
    } yield (proxyRes, thumbRes, metaRes)

    combinedRes.map {
      case (Right(_), Right(_), Right(_)) => Right("Smooth sailing, ATT files removed")
      case (_, _, _) => Left("One or more ATT files could not be removed. Please look at logs for more info")
    }
  }

  def deleteFromNearline(vault: Vault, msg: OnlineOutputMessage): Future[Either[String, MediaRemovedMessage]] = {
    (msg.mediaTier, msg.nearlineId) match {
      case ("NEARLINE", Some(nearlineId)) =>
        dealWithAttFiles(vault, nearlineId, msg)
        // TODO do we need to wrap this with a Future.fromTry?
        Try { vault.getObject(nearlineId).delete() } match {
          case Success(_) =>
            logger.info(s"Nearline media oid=${msg.nearlineId}, path=${msg.filePath} removed")
            Future(Right(MediaRemovedMessage(mediaTier = msg.mediaTier, filePath = msg.filePath, nearlineId = Some(nearlineId), itemId = msg.itemId)))
          case Failure(exception) =>
            logger.warn(s"Failed to remove nearline media oid=${msg.nearlineId}, path=${msg.filePath}, reason: ${exception.getMessage}")
            Future(Left(s"Failed to remove nearline media oid=${msg.nearlineId}, path=${msg.filePath}, reason: ${exception.getMessage}"))
        }
      case (_,_) => throw new RuntimeException(s"Cannot delete from nearline, wrong media tier (${msg.mediaTier}), or missing nearline id (${msg.nearlineId})")
    }
  }

  def storeDeletionPending(msg: OnlineOutputMessage): Future[Either[String, Int]] = {
    (msg.mediaTier, msg.itemId, msg.nearlineId) match {
      case ("NEARLINE", _, Some(nearlineId)) =>
        pendingDeletionRecordDAO
          .findByNearlineId(nearlineId)
          .map({
            case Some(existingRecord)=>
              existingRecord.copy(
                attempt = existingRecord.attempt + 1
              )
            case None=>
              PendingDeletionRecord(
                None,
                mediaTier = MediaTiers.NEARLINE,
                originalFilePath = msg.filePath,
                onlineId = msg.itemId,
                nearlineId = Some(nearlineId),
                attempt = 1)
          })
          .flatMap(rec=>{
            pendingDeletionRecordDAO
              .writeRecord(rec)
              .map(recId => Right(recId))
          }
        )
      case ("NEARLINE", _, _) =>
        Future.failed(new RuntimeException("NEARLINE but no nearlineId"))

      case ("ONLINE", Some(onlineId), _) =>
        logger.warn(s"Not implemented yet - $onlineId ignored")
        Future.failed(SilentDropMessage(Some(s"Not implemented yet - $onlineId ignored")))
      case ("ONLINE", _, _) =>
        Future.failed(new RuntimeException("ONLINE but no onlineId"))

      case (_, _, _) =>
        Future.failed(new RuntimeException("This should not happen!"))
    }
  }

  def ni_outputDeepArchiveCopyRequried(onlineOutputMessage: OnlineOutputMessage): Either[String, NearlineRecord] = ???
  def ni_outputInternalArchiveCopyRequried(onlineOutputMessage: OnlineOutputMessage): Either[String, NearlineRecord] = ???

  def getActionToPerform(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]): (Action.Value, Option[ProjectRecord]) =
    maybeProject match {
      case None => (Action.DropMsg, None)
      case Some(project) =>
        project.deletable match {
          case Some(true) =>
            project.status match {
              case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                onlineOutputMessage.mediaCategory.toLowerCase match {
                  case "deliverables" => (Action.DropMsg, Some(project))
                  case _ => (Action.ClearAndDelete, Some(project))
                }
              case _ => (Action.DropMsg, Some(project))
            }
          case _ =>
          // not DELETABLE
          if (project.deep_archive.getOrElse(false)) {
            if (project.sensitive.getOrElse(false)) {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                (Action.CheckInternalArchive, Some(project))
              } else {
                (Action.DropMsg, Some(project))
              }
            } else {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                (Action.CheckDeepArchive, Some(project))
              } else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                (Action.DropMsg, Some(project))
              }
            }
          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            (Action.JustNo, Some(project))
          }
        }
    }

  private def performAction(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] = {
    def deleteFromNearlineWrapper(project: ProjectRecord): Future[Either[String, MessageProcessorReturnValue]]  = {
      deleteFromNearline(vault, onlineOutputMessage).map({
        case Left(value) => Left(value)
        case Right(value) =>
          logger.debug(s"--> deleting nearline media ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
          Right(value.asJson)
      })
    }

    actionToPerform match {
      case (Action.DropMsg, None) =>
        val noProjectFoundMsg = s"No project could be found that is associated with $onlineOutputMessage, erring on the safe side, not removing"
        logger.warn(noProjectFoundMsg)
        throw SilentDropMessage(Some(noProjectFoundMsg))

      case (Action.DropMsg, Some(project)) =>
        val deletable = project.deletable.getOrElse(false)
        val deep_archive = project.deep_archive.getOrElse(false)
        val sensitive = project.sensitive.getOrElse(false)
        val notRemovingMsg = s"not removing nearline media ${onlineOutputMessage.nearlineId.getOrElse("-1")}, " +
          s"project ${project.id.getOrElse(-1)} is deletable($deletable), deep_archive($deep_archive), " +
          s"sensitive($sensitive), status is ${project.status}, " +
          s"media category is ${onlineOutputMessage.mediaCategory}"
        logger.debug(s"-> $notRemovingMsg")
        throw SilentDropMessage(Some(notRemovingMsg))

      case (Action.CheckDeepArchive, Some(project)) =>
        existsInDeepArchive(vault, onlineOutputMessage).flatMap({
          case true =>
            removeDeletionPendingByMessage(onlineOutputMessage)
            deleteFromNearlineWrapper(project)
          case false =>
            storeDeletionPending(onlineOutputMessage) // TODO do we need to recover if db write fails, or can we let it bubble up?
            ni_outputDeepArchiveCopyRequried(onlineOutputMessage) match {
              case Left(err) => Future(Left(err))
              case Right(msg) =>
                logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                Future(Right(msg.asJson))
            }
        })

      case (Action.CheckInternalArchive, Some(project)) =>
        existsInInternalArchive(vault, internalArchiveVault, onlineOutputMessage).flatMap({
          case true =>
            // media EXISTS in INTERNAL ARCHIVE
            removeDeletionPendingByMessage(onlineOutputMessage)
            deleteFromNearlineWrapper(project)
          case false =>
            // media does NOT EXIST in INTERNAL ARCHIVE
            storeDeletionPending(onlineOutputMessage) // TODO do we need to recover if db write fails, or can we let it bubble up?
            ni_outputInternalArchiveCopyRequried(onlineOutputMessage) match {
              case Left(err) => Future(Left(err))
              case Right(msg) =>
                logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                Future(Right(msg.asJson))
            }
        })

      case (Action.ClearAndDelete, Some(project)) =>
        removeDeletionPendingByMessage(onlineOutputMessage)
        deleteFromNearlineWrapper(project)

      case (Action.JustNo, Some(project)) =>
        logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
        throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")

      case (unexpectedAction, Some(project)) =>
        logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $unexpectedAction")
        throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $unexpectedAction")

      case (unexpectedAction, _) =>
        logger.warn(s"Cannot remove file: unexpected action $unexpectedAction when no project")
        throw new RuntimeException(s"Cannot remove file: unexpected action $unexpectedAction when no project")
    }
  }

//  def bulkGetProjectMetadata(mediaNotRequiredMsg: OnlineOutputMessage) = {
//    mediaNotRequiredMsg.projectIds.map(id => asLookup.getProjectMetadata(id.toString)).sequence
//  }

  def handleNearline(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.filePath, onlineOutputMessage.nearlineId)
    for {
      /* ignore all but the first project - we're only getting the main project as of yet */
      projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head.toString)
      actionToPerform <- Future(getActionToPerform(onlineOutputMessage, projectRecordMaybe))
      fileRemoveResult <- performAction(vault, internalArchiveVault, onlineOutputMessage, actionToPerform)
    } yield fileRemoveResult
  }

  def validateNeededFields(fileSizeMaybe: Option[Long], filePathMaybe: Option[String], nearlineOrOnlineIdMaybe: Option[String]) = {
    (fileSizeMaybe, filePathMaybe, nearlineOrOnlineIdMaybe) match {
      case (None, _, _) => throw new RuntimeException(s"fileSize is missing")
      case (_, None, _) => throw new RuntimeException(s"filePath is missing")
      case (_, _, None) => throw new RuntimeException(s"media id is missing")
      case (Some(-1), _, _) => throw new RuntimeException(s"fileSize is -1")
      case (Some(fileSize), Some(filePath), Some(id)) => (fileSize, filePath, id)
    }
  }

  private def handleOnline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    // Sanity checks
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.filePath, onlineOutputMessage.itemId)
    Future(Left("testing online"))
  }
}

object MediaNotRequiredMessageProcessor {
  object Action extends Enumeration {
    val CheckDeepArchive, CheckInternalArchive, ClearAndDelete, DropMsg, JustNo  = Value
  }
}
