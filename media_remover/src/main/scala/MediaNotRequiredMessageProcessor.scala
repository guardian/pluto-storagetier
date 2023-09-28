import MediaNotRequiredMessageProcessor.Action
import utils.Ensurer.validateNeededFields
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.messages.{OnlineOutputMessage, VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecordDAO
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class MediaNotRequiredMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  nearlineRecordDAO: NearlineRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
  vidispineCommunicator: VidispineCommunicator,
  s3ObjectChecker: S3ObjectChecker,
  checksumChecker: ChecksumChecker,
  onlineHelper: OnlineHelper,
  nearlineHelper: NearlineHelper,
  pendingDeletionHelper: PendingDeletionHelper
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    routingKey match {
      case "storagetier.restorer.media_not_required.nearline" =>
        msg.as[OnlineOutputMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"))
          case Right(onlineOutputMessageNearline) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleNearlineMediaNotRequired(nearlineVault, internalArchiveVault, onlineOutputMessageNearline)
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
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleOnlineMediaNotRequired(nearlineVault, internalArchiveVault, onlineOutputMessageOnline)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }

      case _ =>
        logger.warn(s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it.")
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }


  protected def callFindByFilenameNew(vault:Vault, fileName:String): Future[Seq[ObjectMatrixEntry]] = MatrixStoreHelper.findByFilenameNew(vault, fileName)
  protected def callObjectMatrixEntryFromOID(vault:Vault, fileName:String): Future[ObjectMatrixEntry] = ObjectMatrixEntry.fromOID(fileName, vault)
  protected def openMxsObject(vault:Vault, oid:String): Try[MxsObject] = Try { vault.getObject(oid) }


  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, filePath, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    nearlineHelper.nearlineExistsInInternalArchive(vault, internalArchiveVault, nearlineId, filePath, fileSize)
  }


  def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] =
    for {
      maybeChecksum <- onlineHelper.getMd5ChecksumForOnline(vsItemId)
      exists <- nearlineHelper.existsInTargetVaultWithMd5Match(MediaTiers.ONLINE, vsItemId, nearlineVaultOrInternalArchiveVault, filePath, fileSize, maybeChecksum)
    } yield exists


  def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, originalFilePath, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    val maybeChecksumFut = nearlineHelper.getChecksumForNearline(vault, nearlineId)
    val objectKey = getPossiblyRelativizedObjectKey(onlineOutputMessage.mediaTier, originalFilePath)
    maybeChecksumFut.flatMap(checksumMaybe => s3ObjectChecker.nearlineMediaExistsInDeepArchive(checksumMaybe, fileSize, originalFilePath, objectKey))
  }


  private def getPossiblyRelativizedObjectKey(mediaTier: String, originalFilePath: String) =
      asLookup.relativizeFilePath(Paths.get(originalFilePath)) match {
        case Left(err) =>
          logger.error(s"Could not relativize $mediaTier file path $originalFilePath: $err. Checking ${originalFilePath.stripPrefix("/")}")
          originalFilePath.stripPrefix("/")
        case Right(relativePath) =>
          relativePath.toString
      }


  // online DA
  def outputDeepArchiveCopyRequiredForOnline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] =
    outputDeepArchiveCopyRequiredForOnline(onlineOutputMessage.vidispineItemId.get, onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath)

  def outputDeepArchiveCopyRequiredForOnline(pendingDeletionRecord: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] =
    outputDeepArchiveCopyRequiredForOnline(pendingDeletionRecord.vidispineItemId.get, pendingDeletionRecord.mediaTier.toString, Some(pendingDeletionRecord.originalFilePath))

  def outputDeepArchiveCopyRequiredForOnline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = {
    // We know we have an online item to delete, but no DA
    // -> vidispine.itemneedsbackup ---> vidispine.itemneedsarchive.online because we don't wanna trigger the other copy needlessly
    // -> implement copy from vs a la online_nearline but to S3
    logger.info(s"Outputting Deep Archive copy required for online item (not removing $tier $path)")
    val itemNeedsArchiving = VidispineMediaIngested(List(VidispineField("itemId", itemId)))
    Future(Right(MessageProcessorReturnValue(itemNeedsArchiving.asJson, Seq(RMQDestination("vidispine-events", "vidispine.itemneedsarchive.online")))))
  }


  // nearline DA
  def outputDeepArchiveCopyRequiredForNearline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    onlineOutputMessage.vidispineItemId match {
      case Some(vsItemId) => outputDeepArchiveCopyRequiredForNearline(vsItemId, onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath)
      case None => throw new RuntimeException(s"Cannot reqeust new deep archive for ${onlineOutputMessage.mediaTier}, ${onlineOutputMessage.originalFilePath}, no vidispine id to piggyback on")
    }
  }

  def outputDeepArchiveCopyRequiredForNearline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = {
    // We know we have an online item to delete, but no DA
    // -> vidispine.itemneedsbackup ---> vidispine.itemneedsarchive.nearline because we don't wanna trigger the other copy needlessly
    // -> implement copy from vs a la online_nearline but to S3
    logger.info(s"Outputting Deep Archive copy required for nearline item (not removing $tier $path)")
    val itemNeedsArchiving = VidispineMediaIngested(List(VidispineField("itemId", itemId)))
    Future(Right(MessageProcessorReturnValue(itemNeedsArchiving.asJson, Seq(RMQDestination("vidispine-events", "vidispine.itemneedsarchive.nearline")))))
  }

  // online IA
  def outputInternalArchiveCopyRequiredForOnline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    nearlineHelper.outputInternalArchiveCopyRequiredForOnline(onlineOutputMessage.vidispineItemId.get, onlineOutputMessage.originalFilePath.getOrElse("<no originalFilePath>"))
  }

  // nearline IA
  def outputInternalArchiveCopyRequiredForNearline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] =
    nearlineHelper.outputInternalArchiveCopyRequiredForNearline(onlineOutputMessage.nearlineId.get, onlineOutputMessage.originalFilePath)

  // online nearline
  def dropAndWaitForAssetSweeper(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.debug(s"Dropping message - wait for AssetSweeper to find and copy ${onlineOutputMessage.mediaTier} ${onlineOutputMessage.originalFilePath}) to nearline vault")
    throw SilentDropMessage(Some(s"Dropping message - wait for AssetSweeper to find and copy ${onlineOutputMessage.mediaTier} ${onlineOutputMessage.originalFilePath}) to nearline vault"))
  }


  def getActionToPerformOnline(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord], maybeForceDelete: Option[Boolean]): (Action.Value, Option[ProjectRecord]) = {
    maybeProject match {
      case None =>
        logger.debug(s"Action to perform: '${Action.DropMsg}' for ${getInformativeIdStringNoProject(onlineOutputMessage)}")
        (Action.DropMsg, None)
      case Some(project) =>
        if (maybeForceDelete == Some(true)) {
          logger.info(s"Force delete found set.")
          logAndSelectAction(Action.ClearAndDelete, onlineOutputMessage, project)
        }
        project.status match {
          case status if status == EntryStatus.Held =>
            logAndSelectAction(Action.CheckExistsOnNearline, onlineOutputMessage, project)
          case _ =>
            project.deletable match {
              case Some(true) =>
                project.status match {
                  case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                    // deletable + Completed/Killed
                    logAndSelectAction(Action.ClearAndDelete, onlineOutputMessage, project)
                  case _ =>
                    logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                }
              case _ =>
                // not DELETABLE
                if (project.deep_archive.getOrElse(false)) {
                  if (project.sensitive.getOrElse(false)) {
                    if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                      // not deletable + deep_archive + sensitive + Completed/Killed
                      logAndSelectAction(Action.CheckInternalArchive, onlineOutputMessage, project)
                    } else {
                      logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                    }
                  } else {
                    if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                      // not deletable + deep_archive + not sensitive + Completed/Killed
                      logAndSelectAction(Action.CheckDeepArchiveForOnline, onlineOutputMessage, project)
                    } else {
                      logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                    }
                  }
                } else {
                  // not deletable + not deep_archive
                  logAndSelectAction(Action.JustNo, onlineOutputMessage, project)
                }
            }
        }
    }
  }


  private def logAndSelectAction(action: MediaNotRequiredMessageProcessor.Action.Value, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    logger.info(s"Action to perform: '$action' for ${getInformativeIdString(onlineOutputMessage, project)}")
    (action, Some(project))
  }

  def getActionToPerformNearline(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]): (Action.Value, Option[ProjectRecord]) =
    maybeProject match {
      case None =>
        logger.debug(s"Action to perform: '${Action.DropMsg}' for ${getInformativeIdStringNoProject(onlineOutputMessage)}")
        (Action.DropMsg, None)
      case Some(project) =>
        project.deletable match {
          case Some(true) =>
            project.status match {
              case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                onlineOutputMessage.mediaCategory.toLowerCase match {
                  case "deliverables" => logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                  case _ => logAndSelectAction(Action.ClearAndDelete, onlineOutputMessage, project)
                }
              case _ => logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
            }
          case _ =>
          // not DELETABLE
          if (project.deep_archive.getOrElse(false)) {
            if (project.sensitive.getOrElse(false)) {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                logAndSelectAction(Action.CheckInternalArchive, onlineOutputMessage, project)
              } else {
                logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
              }
            } else {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                logAndSelectAction(Action.CheckDeepArchiveForNearline, onlineOutputMessage, project)
              } else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
              }
            }
          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            logAndSelectAction(Action.JustNo, onlineOutputMessage, project)
          }
        }
    }


  private def performActionNearline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] =
    actionToPerform match {
      case (Action.DropMsg, None) =>
        handleDropMsg(onlineOutputMessage)

      case (Action.DropMsg, Some(project)) =>
        handleDropMsg(onlineOutputMessage, project)

      case (Action.CheckDeepArchiveForNearline, Some(project)) =>
        handleCheckDeepArchiveForNearline(nearlineVault, onlineOutputMessage, project)

      case (Action.CheckInternalArchive, Some(project)) =>
        handleCheckInternalArchiveForNearline(nearlineVault, internalArchiveVault, onlineOutputMessage, project)

      case (Action.ClearAndDelete, Some(project)) =>
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)

      case (Action.JustNo, Some(project)) =>
        handleJustNo(project)

      case (unexpectedAction, Some(project)) =>
        handleUnexpectedAction(unexpectedAction, project)

      case (unexpectedAction, _) =>
        handleUnexpectedAction(unexpectedAction)
    }


  private def handleUnexpectedAction(unexpectedAction: MediaNotRequiredMessageProcessor.Action.Value) = {
    logger.warn(s"Cannot remove file: unexpected action $unexpectedAction when no project")
    throw new RuntimeException(s"Cannot remove file: unexpected action $unexpectedAction when no project")
  }

  private def handleUnexpectedAction(unexpectedAction: MediaNotRequiredMessageProcessor.Action.Value, project: ProjectRecord) = {
    logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $unexpectedAction")
    throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $unexpectedAction")
  }

  private def handleDropMsg(onlineOutputMessage: OnlineOutputMessage) = {
    val noProjectFoundMsg = s"Dropping request to remove media: No project could be found that is associated with ${getInformativeIdStringNoProject(onlineOutputMessage)}"
    logger.warn(noProjectFoundMsg)
    throw SilentDropMessage(Some(noProjectFoundMsg))
  }


  def handleCheckDeepArchiveForNearline(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    nearlineMediaExistsInDeepArchive(nearlineVault, onlineOutputMessage).flatMap({
      case true =>
        logger.debug(s"'${getInformativeIdString(onlineOutputMessage, project)}' exists in Deep Archive, going to delete nearline copy")
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)
      case false =>
        logger.debug(s"'${getInformativeIdString(onlineOutputMessage, project)}' does not exist in Deep Archive, going to store deletion pending and request Deep Archive copy")
        pendingDeletionHelper.storeDeletionPending(onlineOutputMessage)
        outputDeepArchiveCopyRequiredForNearline(onlineOutputMessage)
    }).recover({
      case err: Throwable =>
        logger.info(s"Could not connect to deep archive to check if copy of ${onlineOutputMessage.mediaTier} media exists, do not delete yet. Reason: ${err.getMessage}")
        Left(s"Could not connect to deep archive to check if copy of ${onlineOutputMessage.mediaTier} media exists, do not delete yet. Reason: ${err.getMessage}")
    })
  }


  private def handleCheckInternalArchiveForNearline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) =
    nearlineExistsInInternalArchive(nearlineVault, internalArchiveVault, onlineOutputMessage).flatMap({
      case true =>
        // nearline media EXISTS in INTERNAL ARCHIVE
        logger.debug(s"Exists in Internal Archive, going to delete ${getInformativeIdString(onlineOutputMessage, project)} ")
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)
      case false =>
        // nearline media DOES NOT EXIST in INTERNAL ARCHIVE
        logger.debug(s"Does not exist in Internal Archive, going to store deletion pending and request Internal Archive copy for ${getInformativeIdString(onlineOutputMessage, project)}")
        pendingDeletionHelper.storeDeletionPending(onlineOutputMessage)
        outputInternalArchiveCopyRequiredForNearline(onlineOutputMessage)
    })


  private def handleDeleteNearlineAndClear(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) =
    for {
      mediaRemovedMsg <- deleteFromNearlineWrapper(nearlineVault, onlineOutputMessage, project)
      _ <- pendingDeletionHelper.removeDeletionPendingByMessage(onlineOutputMessage)
    } yield mediaRemovedMsg


  private def handleJustNo(project: ProjectRecord) = {
    logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
    throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
  }


  def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, originalFilePath, vsItemId) =
      validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)
    for {
      checksumMaybe <- onlineHelper.getMd5ChecksumForOnline(vsItemId)
      objectKey <- Future(getPossiblyRelativizedObjectKey(onlineOutputMessage.mediaTier, originalFilePath))
      mediaExists <- s3ObjectChecker.onlineMediaExistsInDeepArchive(checksumMaybe, fileSize, originalFilePath, objectKey)
    } yield mediaExists
  }


  private def handleCheckVaultForOnline(nearlineVaultOrInternalArchiveVault: Vault,
                                        onlineOutputMessage: OnlineOutputMessage,
                                        outputRequiredMsgFn: OnlineOutputMessage => Future[Either[String, MessageProcessorReturnValue]]
                                       ) = {
    val (fileSize, filePath, vsItemId) =
      validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)
    onlineExistsInVault(nearlineVaultOrInternalArchiveVault, vsItemId, filePath, fileSize).flatMap({
        case true => handleDeleteOnlineAndClear(onlineOutputMessage)
        case false =>
          pendingDeletionHelper.storeDeletionPending(onlineOutputMessage)
          outputRequiredMsgFn(onlineOutputMessage)
      })
  }


  private def handleDeleteOnlineAndClear(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    for {
      mediaRemovedMessage <- onlineHelper.deleteMediaFromOnline(onlineOutputMessage)
      _ <- pendingDeletionHelper.removeDeletionPendingByMessage(onlineOutputMessage)
    } yield mediaRemovedMessage
  }


  def handleCheckDeepArchiveForOnline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] =
    onlineMediaExistsInDeepArchive(onlineOutputMessage).flatMap({
      case true =>
        handleDeleteOnlineAndClear(onlineOutputMessage)
      case false =>
        pendingDeletionHelper.storeDeletionPending(onlineOutputMessage)
        outputDeepArchiveCopyRequiredForOnline(onlineOutputMessage)
    }).recover({
      case err: Throwable =>
        logger.warn(s"Could not connect to deep archive to check if copy of ${onlineOutputMessage.mediaTier} media exists, do not delete yet. Reason: ${err.getMessage}")
        Left(s"Could not connect to deep archive to check if copy of ${MediaTiers.NEARLINE} media exists, do not delete yet. Reason: ${err.getMessage}")
    })


  private def handleDropMsg(onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    val notRemovingMsg: String = s"Dropping request to remove ${getInformativeIdString(onlineOutputMessage, project)}"
    logger.debug(notRemovingMsg)
    throw SilentDropMessage(Some(notRemovingMsg))
  }


  private def getInformativeIdString(onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    s"${onlineOutputMessage.originalFilePath.getOrElse("<missing originalFilePath>")}: " +
      s"${onlineOutputMessage.mediaTier} media with " +
      s"nearlineId ${onlineOutputMessage.nearlineId.getOrElse("<missing nearline ID>")}, " +
      s"onlineId ${onlineOutputMessage.vidispineItemId.getOrElse("<missing vsItem ID>")}, " +
      s"mediaCategory ${onlineOutputMessage.mediaCategory} " +
      s"in project ${project.id.getOrElse(-1)}: " +
      s"deletable(${project.deletable.getOrElse(false)}), " +
      s"deep_archive(${project.deep_archive.getOrElse(false)}), " +
      s"sensitive(${project.sensitive.getOrElse(false)}), " +
      s"status ${project.status}"
  }
  private def getInformativeIdStringNoProject(onlineOutputMessage: OnlineOutputMessage) =
        s"${onlineOutputMessage.mediaTier} media with " +
          s"nearlineId ${onlineOutputMessage.nearlineId.getOrElse("<missing nearline ID>")}, " +
          s"onlineId ${onlineOutputMessage.vidispineItemId.getOrElse("<missing vsItem ID>")}, " +
          s"media category is ${onlineOutputMessage.mediaCategory} - Could not find project record for media"


  def deleteFromNearlineWrapper(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    nearlineHelper.deleteMediaFromNearline(nearlineVault, onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId, onlineOutputMessage.vidispineItemId).map({
      case Left(err) =>
        logger.warn(s"Failed to delete ${getInformativeIdString(onlineOutputMessage, project)}. Cause: $err")
        Left(err)
      case Right(mediaRemovedMessage) =>
        logger.debug(s"Deleted ${getInformativeIdString(onlineOutputMessage, project)}")
        Right(mediaRemovedMessage.asJson)
    })
  }


  def handleNearlineMediaNotRequired(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    onlineOutputMessage.mediaTier match {
      case "NEARLINE" =>
        for {
          /* ignore all but the first project - we're only getting the main project as of yet */
          projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head)
          actionToPerform <- Future(getActionToPerformNearline(onlineOutputMessage, projectRecordMaybe))
          fileRemoveResult <- performActionNearline(nearlineVault, internalArchiveVault, onlineOutputMessage, actionToPerform)
        } yield fileRemoveResult
      case notWanted =>
        throw new RuntimeException(s"handleNearlineMediaNotRequired called with unexpected mediaTier: $notWanted")
    }
  }

  def handleOnlineMediaNotRequired(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    // Sanity checks
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)
    onlineOutputMessage.mediaTier match {
      case "ONLINE" =>
        logger.info(s"Force delete setting is ${onlineOutputMessage.forceDelete}")
        for {
          /* ignore all but the first project - we're only getting the main project as of yet */
          projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head)
          actionToPerform <- Future(getActionToPerformOnline(onlineOutputMessage, projectRecordMaybe, onlineOutputMessage.forceDelete))
          fileRemoveResult <- performActionOnline(vault, internalArchiveVault, onlineOutputMessage, actionToPerform)
        } yield fileRemoveResult
      case notWanted =>
        throw new RuntimeException(s"handleOnlineMediaNotRequired called with unexpected mediaTier: $notWanted")
    }
  }


  def performActionOnline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (MediaNotRequiredMessageProcessor.Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] = {
    actionToPerform match {
      case (Action.CheckDeepArchiveForOnline, Some(_)) =>
        handleCheckDeepArchiveForOnline(onlineOutputMessage)

      case (Action.CheckInternalArchive, Some(_)) =>
        handleCheckVaultForOnline(internalArchiveVault, onlineOutputMessage, outputInternalArchiveCopyRequiredForOnline)

      case (Action.CheckExistsOnNearline, Some(_)) =>
        handleCheckVaultForOnline(nearlineVault, onlineOutputMessage, dropAndWaitForAssetSweeper)

      case (Action.ClearAndDelete, Some(_)) =>
        handleDeleteOnlineAndClear(onlineOutputMessage)

      case (Action.DropMsg, None) =>
        handleDropMsg(onlineOutputMessage)

      case (Action.DropMsg, Some(project)) =>
        handleDropMsg(onlineOutputMessage, project)

      case (Action.JustNo, Some(project)) =>
        handleJustNo(project)

      case (unexpectedAction, Some(project)) =>
        handleUnexpectedAction(unexpectedAction, project)

      case (unexpectedAction, _) =>
        handleUnexpectedAction(unexpectedAction)
    }
  }

}


object MediaNotRequiredMessageProcessor {
  object Action extends Enumeration {
    val CheckDeepArchiveForNearline, CheckInternalArchive, ClearAndDelete, DropMsg, JustNo, CheckExistsOnNearline, CheckDeepArchiveForOnline  = Value
  }
}
