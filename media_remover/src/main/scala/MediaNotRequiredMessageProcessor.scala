import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.implicits.toTraverseOps
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
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
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
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

      case (Right(onlineNotRequired), "storagetier.restorer.media_not_required.online") =>
        handleOnline(onlineNotRequired)

      case (_, _) =>
        logger.warn(
          s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it. This should be fixed in the code."
        )
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }
  }

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
      fileUploader.objectExistsWithSizeAndMaybeChecksum(objectKey, fileSize, maybeChecksum) match {
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

  def _removeDeletionPending(onlineOutputMessage: OnlineOutputMessage): Either[String, String] = ???


  def deleteFromNearline(vault: Vault, msg: OnlineOutputMessage): Future[Either[String, MediaRemovedMessage]] = {
    (msg.mediaTier, msg.nearlineId) match {
      case ("NEARLINE", Some(nearlineId)) =>
        // TODO find and delete any ATT-files before we delete this, the main media file
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

  def _outputDeepArchiveCopyRequried(onlineOutputMessage: OnlineOutputMessage): Either[String, NearlineRecord] = ???

  def _existsInInternalArchive(onlineOutputMessage: OnlineOutputMessage): Boolean = ???

  def _outputInternalArchiveCopyRequried(onlineOutputMessage: OnlineOutputMessage): Either[String, NearlineRecord] = ???


  def getActionToPerform(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]): Future[(MediaNotRequiredMessageProcessor.Action.Value, Option[ProjectRecord])] = {
    val actionToPerform = maybeProject match {
      case None =>
        (MediaNotRequiredMessageProcessor.Action.DropMsg, None)
      case Some(project) =>
        project.deletable match {
          case Some(true) =>
            project.status match {
              case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                onlineOutputMessage.mediaCategory.toLowerCase  match {
                  case "deliverables" => (MediaNotRequiredMessageProcessor.Action.DropMsg, Some(project))
                  case _ => (MediaNotRequiredMessageProcessor.Action.ClearAndDelete, Some(project))
                }
              case _ => (MediaNotRequiredMessageProcessor.Action.DropMsg, Some(project))
            }
          case _ =>
          // not DELETABLE
          if (project.deep_archive.getOrElse(false)) {
            if (project.sensitive.getOrElse(false)) {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                (MediaNotRequiredMessageProcessor.Action.CheckInternalArchive, Some(project))
              } else {
                (MediaNotRequiredMessageProcessor.Action.DropMsg, Some(project))
              }
            } else {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                (MediaNotRequiredMessageProcessor.Action.CheckDeepArchive, Some(project))
              } else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                (MediaNotRequiredMessageProcessor.Action.DropMsg, Some(project))
              }
            }
          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            (MediaNotRequiredMessageProcessor.Action.JustNo, Some(project))
          }
        }
    }

    logger.debug(s"---> actionToPerform._1: ${actionToPerform._1}")
    Future(actionToPerform)
  }

  private def performAction(vault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (MediaNotRequiredMessageProcessor.Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] = {
    actionToPerform match {
      case (MediaNotRequiredMessageProcessor.Action.DropMsg, None) =>
        val noProjectFoundMsg = s"No project could be found that is associated with $onlineOutputMessage, erring on the safe side, not removing"
        logger.warn(noProjectFoundMsg)
        throw SilentDropMessage(Some(noProjectFoundMsg))

      case (MediaNotRequiredMessageProcessor.Action.DropMsg, Some(project)) =>
        val deletable = project.deletable.getOrElse(false)
        val deep_archive = project.deep_archive.getOrElse(false)
        val sensitive = project.sensitive.getOrElse(false)
        val notRemovingMsg = s"not removing nearline media ${onlineOutputMessage.nearlineId.getOrElse("-1")}, " +
          s"project ${project.id.getOrElse(-1)} is deletable($deletable), deep_archive($deep_archive), " +
          s"sensitive($sensitive), status is ${project.status}, " +
          s"media category is ${onlineOutputMessage.mediaCategory}"
        logger.debug(s"-> $notRemovingMsg")
        throw SilentDropMessage(Some(notRemovingMsg))

      case (MediaNotRequiredMessageProcessor.Action.CheckDeepArchive, Some(project)) =>
        existsInDeepArchive(vault, onlineOutputMessage).flatMap({
          case true =>
            _removeDeletionPending(onlineOutputMessage)
            deleteFromNearline(vault, onlineOutputMessage).map({
              case Left(value) => Left(value)
              case Right(value) => Right(value.asJson)
            })
          case false =>
            storeDeletionPending(onlineOutputMessage) // TODO do we need to recover if db write fails, or can we let it bubble up?
            _outputDeepArchiveCopyRequried(onlineOutputMessage) match {
              case Left(err) => Future(Left(err))
              case Right(msg) =>
                logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
                Future(Right(msg.asJson))
            }
        })


      case (MediaNotRequiredMessageProcessor.Action.CheckInternalArchive, Some(project)) =>
        if (_existsInInternalArchive(onlineOutputMessage)) {
          // media EXISTS in INTERNAL ARCHIVE
          _removeDeletionPending(onlineOutputMessage)
          deleteFromNearline(vault, onlineOutputMessage).map({
            case Left(value) => Left(value)
            case Right(value) => Right(value.asJson)
          })
        } else {
          // media does NOT EXIST in INTERNAL ARCHIVE
          storeDeletionPending(onlineOutputMessage) // TODO do we need to recover if db write fails, or can we let it bubble up?
          _outputInternalArchiveCopyRequried(onlineOutputMessage) match {
            case Left(err) => Future(Left(err))
            case Right(msg) =>
              logger.debug(s"--> outputting deep archive copy request for ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
              Future(Right(msg.asJson))
          }
        }


      case (MediaNotRequiredMessageProcessor.Action.ClearAndDelete, Some(project)) =>
        // TODO Remove "pending deletion" record if exists
        _removeDeletionPending(onlineOutputMessage)
        deleteFromNearline(vault, onlineOutputMessage).map({
          case Left(err) => Left(err)
          case Right(msg) =>
            logger.debug(s"--> deleting nearline media ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
            Right(msg.asJson)
        })

      case (MediaNotRequiredMessageProcessor.Action.JustNo, Some(project)) =>
        logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
        throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")

      case (action, Some(project)) =>
        logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $action")
        throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $action")

      case (action, _) =>
        logger.warn(s"Cannot remove file: unexpected action $action when no project")
        throw new RuntimeException(s"Cannot remove file: unexpected action $action when no project")
    }
  }

  def bulkGetProjectMetadata(mediaNotRequiredMsg: OnlineOutputMessage) = {
    mediaNotRequiredMsg.projectIds.map(id => asLookup.getProjectMetadata(id.toString)).sequence
  }

  def handleNearline(vault: Vault, mediaNotRequiredMsg: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    /*
    TODO
     handle seq of projects
     if any is new or in production, then silentdrop
     if at least one sensitive, check if on internalArchive
     if at least one deep_archive, check if on deepArchive
*/
    val projectRecordMaybesFut = bulkGetProjectMetadata(mediaNotRequiredMsg).map(_.map(_.get))

    for {
      projectRecordMaybe <- projectRecordMaybesFut.map(_.headOption)
      actionToPerform <- getActionToPerform(mediaNotRequiredMsg, projectRecordMaybe)
      fileRemoveResult <- performAction(vault, mediaNotRequiredMsg, actionToPerform)
    } yield fileRemoveResult
  }

  private def handleOnline(mediaNotRequiredMsg: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] =
    Future(Left("testing online"))

}

  object MediaNotRequiredMessageProcessor {
    object Action extends Enumeration {
      val CheckDeepArchive, CheckInternalArchive, ClearAndDelete, DropMsg, JustNo  = Value
    }
  }
