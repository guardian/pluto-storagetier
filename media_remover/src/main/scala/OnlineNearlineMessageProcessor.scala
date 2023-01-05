import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.PendingDeletionRecordDAO
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
import com.gu.multimedia.storagetier.plutocore.AssetFolderLookup
import com.om.mxs.client.japi.Vault
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class OnlineNearlineMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
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
      exists <- nearlineHelper.existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, internalArchiveVault, filePathBack, fileSize, maybeChecksum)
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
      exists <- nearlineHelper.existsInTargetVaultWithMd5Match(MediaTiers.ONLINE, vsItemId, nearlineVaultOrInternalArchiveVault, filePath, fileSize, onlineChecksumMaybe)
    } yield exists

}

