import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.implicits.toTraverseOps
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.{LoggerFactory, MDC}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
  fileUploader: FileUploader
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] = {
    (msg.as[OnlineOutputMessage], routingKey) match {
      case (Left(err), _) =>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"))

      case (Right(nearlineNotRequired), "storagetier.restorer.media_not_required.nearline") =>
        matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
          handleNearline(vault, nearlineNotRequired)
        }

      case (
            Right(onlineNotRequired),
            "storagetier.restorer.media_not_required.online"
          ) =>
        handleOnline(onlineNotRequired)
//        Future(Left("handleOnline not implemented yet"))

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


  //TODO loopa igenom projects to find most important action/blocker AKA finish up GP-780
  //TODO use the results of the not exists on deep archive to store pending deletion record


  def getChecksumForNearlineItem(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Option[String]] = {
    for {
      maybeOid <- Future(onlineOutputMessage.nearlineId.get) // we should be able to trust that there is a oid, since we get this message from project_restorer for specifically a nearline item we fetched
      // TODO should we handle the None oid case gracefully, and if so, how? As is, throws am unhandled NoSuchElementException
      maybeMxsFile <- Future.fromTry(Try {vault.getObject(maybeOid)})
      maybeMd5 <- MatrixStoreHelper.getOMFileMd5(maybeMxsFile).flatMap({
            case Failure(err) =>
              logger.error(s"Unable to get checksum from appliance, file should be considered unsafe", err)
              Future(None)
            case Success(remoteChecksum) =>
              logger.info(s"Appliance reported checksum of $remoteChecksum")
              Future(Some(remoteChecksum))
          })
      } yield maybeMd5
  }

  def existsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val fileSize = onlineOutputMessage.fileSize.getOrElse(0L) //TODO just say no? what does it mean that a file has no size?
    val objectKey = onlineOutputMessage.filePath.getOrElse("nopath")
    val maybeChecksumFut = getChecksumForNearlineItem(vault, onlineOutputMessage)
    maybeChecksumFut.map(maybeChecksum =>
      fileUploader.objectExistsWithSizeAndMaybeChecksum(objectKey,  fileSize, maybeChecksum) match {
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

  def _removeDeletionPending(onlineOutputMessage: OnlineOutputMessage) :Either[String, String] = ???

  def _deleteFromNearline(onlineOutputMessage: OnlineOutputMessage): Either[String, MediaRemovedMessage] = ???

  def _storeDeletionPending(onlineOutputMessage: OnlineOutputMessage): Either[String, Boolean] = ???

  def _outputDeepArchiveCopyRequried(onlineOutputMessage: OnlineOutputMessage): Either[String, NearlineRecord] = ???

  def _existsInInternalArchive(onlineOutputMessage: OnlineOutputMessage): Boolean = ???

  def _outputInternalArchiveCopyRequried(onlineOutputMessage: OnlineOutputMessage): Either[String, NearlineRecord] = ???


  def processNearlineFileAndProject(vault: Vault, onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]):Future[Either[String, MessageProcessorReturnValue]] = {
    val actionToPerform = maybeProject match {
      case None =>
        ("drop_msg", None)
      case Some(project) =>
        project.deletable match {
          case Some(true) =>
            project.status match {
              case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                onlineOutputMessage.mediaCategory.toLowerCase  match {
                  case "deliverables" => ("drop_msg", Some(project))
                  case _ => ("clear_and_delete", Some(project))
                }
              case _ => ("drop_msg", Some(project))
            }
          case _ =>
          // not DELETABLE
          if (project.deep_archive.getOrElse(false)) {
            if (project.sensitive.getOrElse(false)) {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {("check_internal_archive", Some(project))}
              else {("drop_msg", Some(project))}
            } else {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                ("check_deep_archive", Some(project))
              } else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                ("drop_msg", Some(project))
              }
            }
          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            ("just_no", Some(project))
          }
        }
    }

    logger.debug(s"---> actionToPerform: ${actionToPerform._1}")

    performAction(vault, onlineOutputMessage, actionToPerform)
  }

  private def performAction(vault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (String, Option[ProjectRecord])) = {
    val resultOfAction = actionToPerform match {
      case ("drop_msg", None) =>
        val noProjectFoundMsg = s"No project could be found that is associated with $onlineOutputMessage, erring on the safe side, not removing"
        logger.warn(noProjectFoundMsg)
        throw SilentDropMessage(Some(noProjectFoundMsg))

      case ("drop_msg", Some(project)) =>
        val deletable = project.deletable.getOrElse(false)
        val deep_archive = project.deep_archive.getOrElse(false)
        val sensitive = project.sensitive.getOrElse(false)
        val notRemovingMsg = s"not removing nearline media ${onlineOutputMessage.nearlineId.getOrElse("-1")}, " +
          s"project ${project.id.getOrElse(-1)} is deletable($deletable), deep_archive($deep_archive), " +
          s"sensitive($sensitive), status is ${project.status}, " +
          s"media category is ${onlineOutputMessage.mediaCategory}"
        logger.debug(s"-> $notRemovingMsg")
        throw SilentDropMessage(Some(notRemovingMsg))

      case ("check_deep_archive", Some(project)) =>
        existsInDeepArchive(vault, onlineOutputMessage).map({
           case true =>
            _removeDeletionPending(onlineOutputMessage)
            _deleteFromNearline(onlineOutputMessage) match {
              case Left(err) => Left(err)
              case Right(msg) =>
                logger.debug(s"--> deleting ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
            }
           case _ =>
            _storeDeletionPending(onlineOutputMessage)
            _outputDeepArchiveCopyRequried(onlineOutputMessage) match {
              case Left(err) => Left(err)
              case Right(msg) =>
                logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
          }
        })


      case ("check_internal_archive", Some(project)) =>
        if (_existsInInternalArchive(onlineOutputMessage)) {
          // media EXISTS in INTERNAL ARCHIVE
          _removeDeletionPending(onlineOutputMessage)
          _deleteFromNearline(onlineOutputMessage) match {
            case Left(err) => Future(Left(err))
            case Right(msg) =>
              logger.debug(s"--> deleting ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
              Future(Right(MessageProcessorConverters.contentToMPRV(msg.asJson)))
          }
        } else {
          // media does NOT EXIST in INTERNAL ARCHIVE
          _storeDeletionPending(onlineOutputMessage)
          _outputInternalArchiveCopyRequried(onlineOutputMessage) match {
            case Left(err) => Future(Left(err))
            case Right(msg) =>
              logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
              Future(Right(MessageProcessorConverters.contentToMPRV(msg.asJson)))
          }
        }

      case ("clear_and_delete", Some(project)) =>
        // TODO Remove "pending deletion" record if exists
        _removeDeletionPending(onlineOutputMessage)
        // TODO Delete media from storage
        _deleteFromNearline(onlineOutputMessage) match {
          case Left(err) => Future(Left(err))
          case Right(msg) =>
            logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
            Future(Right(MessageProcessorConverters.contentToMPRV(msg.asJson)))
        }

      case ("just_no", Some(project)) =>
        logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
        throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")

    }
    resultOfAction
  }

  def bulkGetProjectMetadata(mediaNotRequiredMsg: OnlineOutputMessage) = {
    mediaNotRequiredMsg.projectIds.map(id => asLookup.getProjectMetadata(id.toString)).sequence
  }

  def handleNearline(vault: Vault, mediaNotRequiredMsg: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    /*
    TODO
     handle seq of projects
     if any is new or in production, then silentdrop
     do we need to handle differing statuses other than that, say if differeing in sensitivity?
     may we have to send out deepArchiveRequest AND internalArchiveRequest for the same file??
     -> ?
     if at least one sensitive, check if on internalArchive
     if at least one deep_archive, check if on deepArchive
*/
    val projectRecordMaybesFut = bulkGetProjectMetadata(mediaNotRequiredMsg).map(_.map(_.get))

    for {
      projectRecordMaybe <- projectRecordMaybesFut.map(_.headOption)
      fileRemoveResult <- processNearlineFileAndProject(
        vault,
        mediaNotRequiredMsg,
        projectRecordMaybe
      )
    } yield fileRemoveResult
  }

  private def handleOnline(mediaNotRequiredMsg: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] =
    Future(Left("testing online"))
}
