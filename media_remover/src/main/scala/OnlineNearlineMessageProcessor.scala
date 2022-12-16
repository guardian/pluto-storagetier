import utils.Ensurer.validateNeededFields
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.helpers.{MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.messages.{OnlineOutputMessage, VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.{NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class OnlineNearlineMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
//  nearlineRecordDAO: NearlineRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
//  vidispineCommunicator: VidispineCommunicator,
//  s3ObjectChecker: S3ObjectChecker,
//  checksumChecker: ChecksumChecker,
  onlineHelper: OnlineHelper,
  nearlineHelper: NearlineHelper,
  pendingDeletionHelper: PendingDeletionHelper
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    routingKey match {

      case "storagetier.nearline.internalarchive.nearline.success" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a NearlineRecord: $err"))
          case Right(nearlineRecord) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleInternalArchiveCompleteForNearline(nearlineVault, internalArchiveVault, nearlineRecord)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to check if item is backed-up on internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }

      case "storagetier.nearline.newfile.success" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a NearlineRecord: $err"))
          case Right(nearlineRecord) =>
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
              handleNearlineCompleteForOnline(vault, nearlineRecord)
            }
        }


      case "storagetier.nearline.internalarchive.online.success" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a NearlineRecord: $err"))
          case Right(nearlineRecord) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultFuture(internalArchiveVaultId) { internalArchiveVault =>
                  handleInternalArchiveCompleteForOnline(internalArchiveVault, nearlineRecord)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to check if item is backed-up on internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }

      case _ =>
        logger.warn(s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it.")
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }


  def handleInternalArchiveCompleteForNearline(vault: Vault, internalArchiveVault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] =
    pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(nearlineRecord.objectId).flatMap({
      case Some(rec) =>
        rec.nearlineId match {
          case Some(nearlineId) =>
            val exists = for {
              fileSize <- nearlineHelper.getNearlineFileSize(vault, nearlineId)
              exists <- nearlineExistsInInternalArchive(vault, internalArchiveVault, nearlineId, rec.originalFilePath, fileSize)
            } yield exists

            exists.flatMap({
              case true =>
                for {
                  mediaRemovedMsg <- nearlineHelper.deleteMediaFromNearline(vault, rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, rec.vidispineItemId)
                  _ <- pendingDeletionRecordDAO.deleteRecord(rec)
                } yield mediaRemovedMsg
              case false =>
                pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
                nearlineHelper.outputInternalArchiveCopyRequiredForNearline(rec.nearlineId.get, Some(rec.originalFilePath))
            })

          case None => throw new RuntimeException("NEARLINE pending deletion record w/o nearline id!")
        }
      case None =>
        throw SilentDropMessage(Some(s"ignoring internal archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${nearlineRecord.originalFilePath}"))
    })

  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, nearlineId: String, filePath: String, fileSize: Long): Future[Boolean] = {
    val filePathBack = nearlineHelper.putItBack(filePath)
    for {
      maybeChecksum <- nearlineHelper.getChecksumForNearline(vault, nearlineId)
      //TODO add nearlineId to parameter list for logging purposes(?)
      exists <- nearlineHelper.existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, internalArchiveVault, filePathBack, filePathBack, fileSize, maybeChecksum)
    } yield exists
  }


  def handleInternalArchiveCompleteForOnline(internalArchiveVault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    nearlineRecord.vidispineItemId match {
      case None => throw new RuntimeException("Internal archive completed for online, but no online id!")
      case Some(vsItemId) =>
        pendingDeletionRecordDAO.findByOnlineIdForONLINE(vsItemId).flatMap({
          case Some(rec) =>
            onlineHelper.getOnlineSize(vsItemId).flatMap({
              case Some(onlineSize) =>
                val filePathBack = nearlineHelper.putItBack(rec.originalFilePath)
                onlineExistsInVault(internalArchiveVault, vsItemId, filePathBack, onlineSize).flatMap({
                  case true =>
                    for {
                      mediaRemovedMessage <- onlineHelper.deleteMediaFromOnline(rec)
                      _ <- pendingDeletionRecordDAO.deleteRecord(rec)
                    } yield mediaRemovedMessage
                  case false =>
                    pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
                    nearlineHelper.outputInternalArchiveCopyRequiredForOnline(vsItemId, rec.originalFilePath)
                })
              case _ =>
                logger.info(s"Could not get online size from Vidispine, retrying")
                Future(Left(s"Could not get online size from Vidispine, retrying"))
            })
          case None => throw SilentDropMessage(Some(s"ignoring internal archive confirmation, no pending deletion for this ${MediaTiers.ONLINE.toString} item with ${nearlineRecord.originalFilePath}"))
        })
    }
  }

  def handleNearlineCompleteForOnline(vault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    nearlineRecord.vidispineItemId match {
      case None => throw new RuntimeException("Nearline newfile received for online, but no online id!")
      case Some(vsItemId) =>
        pendingDeletionRecordDAO.findByOnlineIdForONLINE(vsItemId).flatMap({
          case Some(rec) =>
            onlineHelper.getOnlineSize(vsItemId).flatMap({
              case Some(onlineSize) =>
                val filePathBack = nearlineHelper.putItBack(rec.originalFilePath)
                onlineExistsInVault(vault, vsItemId, filePathBack, onlineSize).flatMap({
                  case true =>
                    for {
                      mediaRemovedMessage <- onlineHelper.deleteMediaFromOnline(rec)
                      _ <- pendingDeletionRecordDAO.deleteRecord(rec)
                    } yield mediaRemovedMessage
                  case false =>
                    pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
                    logger.debug(s"Could not find exact match in nearline vault for $vsItemId, size $onlineSize, ${rec.originalFilePath}, wait for next newfile.success")
                    throw SilentDropMessage(Some("Could not find exact match in nearline vault, wait for next newfile.success"))
                })
              case _ =>
                logger.info(s"Could not get online size from Vidispine, retrying")
                Future(Left(s"Could not get online size from Vidispine, retrying"))
            })
          case None => throw SilentDropMessage(Some(s"ignoring nearline newfile confirmation, no pending deletion for this ${MediaTiers.ONLINE.toString} item with ${nearlineRecord.originalFilePath}"))
        })
    }
  }

  def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] =
    for {
      onlineChecksumMaybe <- onlineHelper.getMd5ChecksumForOnline(vsItemId)
      exists <- nearlineHelper.existsInTargetVaultWithMd5Match(MediaTiers.ONLINE, vsItemId, nearlineVaultOrInternalArchiveVault, filePath, filePath, fileSize, onlineChecksumMaybe)
    } yield exists


//  // online IA
//  def outputInternalArchiveCopyRequiredForOnline(vidispineItemId: String, originalFilePath: String): Future[Either[String, MessageProcessorReturnValue]] =
//    nearlineRecordDAO.findByVidispineId(vidispineItemId).map {
//      case Some(rec) => Right(MessageProcessorReturnValue(rec.asJson, Seq(RMQDestination("storagetier-media-remover", "storagetier.online.internalarchive.required")))) //FIXME wrong exchange
//      case None => throw new RuntimeException(s"Cannot request internalArchiveCopy, no record found for ${vidispineItemId} ${originalFilePath}")
//    }
//
//  // nearline IA
//  def outputInternalArchiveCopyRequiredForNearline(oid: String, originalFilePathMaybe: Option[String]): Future[Either[String, MessageProcessorReturnValue]] =
//    nearlineRecordDAO.findBySourceFilename(originalFilePathMaybe.get).map {
//      case Some(rec) => Right(MessageProcessorReturnValue(rec.asJson, Seq(RMQDestination("storagetier-media-remover", "storagetier.nearline.internalarchive.required")))) //FIXME wrong exchange
//      case None => throw new RuntimeException(s"Cannot request internalArchiveCopy, no record found for oid ${oid} ${originalFilePathMaybe}")
//    }


//  private def getInformativeIdString(onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
//    s"${onlineOutputMessage.originalFilePath.getOrElse("<missing originalFilePath>")}: " +
//      s"${onlineOutputMessage.mediaTier} media with " +
//      s"nearlineId ${onlineOutputMessage.nearlineId.getOrElse("<missing nearline ID>")}, " +
//      s"onlineId ${onlineOutputMessage.vidispineItemId.getOrElse("<missing vsItem ID>")}, " +
//      s"mediaCategory ${onlineOutputMessage.mediaCategory} " +
//      s"in project ${project.id.getOrElse(-1)}: " +
//      s"deletable(${project.deletable.getOrElse(false)}), " +
//      s"deep_archive(${project.deep_archive.getOrElse(false)}), " +
//      s"sensitive(${project.sensitive.getOrElse(false)}), " +
//      s"status ${project.status}"
//  }
//  private def getInformativeIdStringNoProject(onlineOutputMessage: OnlineOutputMessage) =
//        s"${onlineOutputMessage.mediaTier} media with " +
//          s"nearlineId ${onlineOutputMessage.nearlineId.getOrElse("<missing nearline ID>")}, " +
//          s"onlineId ${onlineOutputMessage.vidispineItemId.getOrElse("<missing vsItem ID>")}, " +
//          s"media category is ${onlineOutputMessage.mediaCategory} - Could not find project record for media"

//  private def getInformativePendingDeletionString(project: PendingDeletionRecord) = {
//    val id = project.id.getOrElse("<missing rec ID>")
//    val nearlineId = project.nearlineId.getOrElse("<missing nearline ID>")
//    val vsItemId = project.vidispineItemId.getOrElse("<missing vsItem ID>")
//
//    s"${project.mediaTier} pendingDeletionRecord with " +
//      s"id $id, " +
//      s"nearlineId $nearlineId, " +
//      s"onlineId $vsItemId " +
//      s"originalFilePath '${project.originalFilePath}', " +
//      s"attempt ${project.attempt}"
//  }

//  def deleteFromNearlineWrapper(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord): Future[Either[String, MessageProcessorReturnValue]] = {
//    nearlineHelper.deleteMediaFromNearline(nearlineVault, onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId, onlineOutputMessage.vidispineItemId).map({
//      case Left(err) =>
//        logger.warn(s"Failed to delete ${getInformativeIdString(onlineOutputMessage, project)}. Cause: $err")
//        Left(err)
//      case Right(mediaRemovedMessage) =>
//        logger.debug(s"Deleted ${getInformativeIdString(onlineOutputMessage, project)}")
//        Right(mediaRemovedMessage.asJson)
//    })
//  }


//  private def getPendingMaybeDeletionRecord(archivedRecord: ArchivedRecord): Future[Option[PendingDeletionRecord]] =
//    (for {
//      byOriginal <- pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(archivedRecord.originalFilePath, MediaTiers.NEARLINE)
//      byUploaded <- pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(archivedRecord.uploadedPath, MediaTiers.NEARLINE)
//      // byVsItemId <- pendingDeletionRecordDAO.findByOnlineIdForNEARLINE(archivedRecord.vidispineItemId // maybe?)
//    } yield (byOriginal, byUploaded)).map {
//      case (Some(o), _) => Some(o)
//      case (_, Some(u)) => Some(u)
//      case _ => None
//    }

}

