import MediaNotRequiredMessageProcessor.Action
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
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.LoggerFactory
import utils.Ensurer

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class OnlineArchiveMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  nearlineRecordDAO: NearlineRecordDAO,
  ec: ExecutionContext,
//  mat: Materializer,
//  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
//  vidispineCommunicator: VidispineCommunicator,
  s3ObjectChecker: S3ObjectChecker,
//  checksumChecker: ChecksumChecker,
  onlineHelper: OnlineHelper,
  nearlineHelper: NearlineHelper,
  pendingDeletionHelper: PendingDeletionHelper
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    routingKey match {

      // GP-786 Deep Archive complete handler
      // GP-787 Archive complete handler
      case "storagetier.onlinearchive.mediaingest.nearline" =>
        msg.as[ArchivedRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
              handleDeepArchiveCompleteForNearline(vault, archivedRecord)
            }
        }

      case "storagetier.onlinearchive.mediaingest.online" =>
        msg.as[ArchivedRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
            handleDeepArchiveCompleteForOnline(archivedRecord)
        }

      case _ =>
        logger.warn(s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it.")
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }


  def handleDeepArchiveCompleteForNearline(vault: Vault, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    // FIXME It seems we do not have nearlineId in an archiveRecord, and as we've previously noticed, originalFilePath can be the same for more than one nearline media item
    // TODO Alright, if we get a AR for a nearline item, fetch all nearline pending records for that originalFilePath.. and.. what.. check all?
    // Search for originalFilePath AND uploadedPath. If either of those matches, use the the found PendingDeletionRecord as source.

    findPendingDeletionRecord(archivedRecord).flatMap({
      case Some(rec) =>
        // We have a record indication we want to delete this media item. Now, check if it actually was backed-up were it should be
        rec.nearlineId match {
          case Some(nearlineId) =>
            val doesMediaExist =
              for {
                // We fetch the current size, because we don't know how old the message is
                nearlineFileSize <- nearlineHelper.getNearlineFileSize(vault, nearlineId)
                (fileSize, originalFilePath, nearlineId) = Ensurer.validateNeededFields(Some(nearlineFileSize), Some(rec.originalFilePath), rec.nearlineId)
                objectKey = getPossiblyRelativizedObjectKey(MediaTiers.NEARLINE.toString, originalFilePath)
                checksumMaybe <- nearlineHelper.getChecksumForNearline(vault, nearlineId)
                doesMediaExist <- s3ObjectChecker.nearlineMediaExistsInDeepArchive(checksumMaybe, fileSize, originalFilePath, objectKey)
              } yield doesMediaExist

            doesMediaExist.flatMap({
              case true =>
                for {
                  result <- nearlineHelper.deleteMediaFromNearline(vault, rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, rec.vidispineItemId)
                  _ <- pendingDeletionRecordDAO.deleteRecord(rec)
                } yield result
              case false =>
                pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
//                NOT_IMPL_outputDeepArchiveCopyRequiredForNearline(rec)
                rec.vidispineItemId match {
                  case Some(vsItemId) => outputDeepArchiveCopyRequiredForNearline(vsItemId, rec.mediaTier.toString, Some(rec.originalFilePath))
                  case None => throw SilentDropMessage(Some(s"Cannot request deep archive copy for ${rec.mediaTier.toString}, ${rec.originalFilePath}, $nearlineId - missing vidispine ID!"))
                }
            })
          case None =>
            logger.warn(s"No nearline ID in pending deletion rec for media ${archivedRecord.originalFilePath}")
            throw SilentDropMessage(Some(s"No nearline ID in pending deletion rec for media ${archivedRecord.originalFilePath}"))
        }
      case _ =>
        logger.debug(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}")
        throw SilentDropMessage(Some(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}"))
    })
  }


  def handleDeepArchiveCompleteForOnline(archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    val (_, _, vsItemId) = Ensurer.validateNeededFields(Some(1), Some("path"), archivedRecord.vidispineItemId)
    pendingDeletionRecordDAO.findByOnlineIdForONLINE(vsItemId).flatMap({
      case Some(rec) =>
        onlineHelper.getOnlineSize(vsItemId).flatMap(sizeMaybe => {
          val (fileSize, originalFilePath, _) = Ensurer.validateNeededFields(sizeMaybe, Some(rec.originalFilePath), Some(vsItemId))
          onlineHelper.getMd5ChecksumForOnline(vsItemId).flatMap(checksumMaybe => {
            val objectKey = getPossiblyRelativizedObjectKey(MediaTiers.ONLINE.toString, originalFilePath)
            s3ObjectChecker.onlineMediaExistsInDeepArchive(checksumMaybe, fileSize, originalFilePath, objectKey).flatMap({
              case true =>
                for {
                  mediaRemovedMsg <- onlineHelper.deleteMediaFromOnline(rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, Some(vsItemId))
                  _ <- pendingDeletionRecordDAO.deleteRecord(rec)
                } yield mediaRemovedMsg
              case false =>
                pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
                outputDeepArchiveCopyRequiredForOnline(rec.vidispineItemId.get, rec.mediaTier.toString, Some(rec.originalFilePath))
            })
          })
        })
      case None =>
        throw SilentDropMessage(Some(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}"))
    })
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
  def outputDeepArchiveCopyRequiredForOnline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = {
    // We know we have an online item to delete, but no DA
    // -> vidispine.itemneedsbackup ---> vidispine.itemneedsarchive because we don't wanna trigger the other copy needlessly
    // -> implement copy from vs a la online_nearline but to S3
    logger.info(s"Outputting Deep Archive copy required for online item (not removing ${tier} ${path})")
    val itemNeedsArchiving = VidispineMediaIngested(List(VidispineField("itemId", itemId)))
    Future(Right(MessageProcessorReturnValue(itemNeedsArchiving.asJson, Seq(RMQDestination("vidispine-events", "vidispine.itemneedsarchive.online")))))
  }

  def outputDeepArchiveCopyRequiredForNearline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"Outputting Deep Archive copy required for nearline item (not removing ${tier} ${path})")
    val itemNeedsArchiving = VidispineMediaIngested(List(VidispineField("itemId", itemId)))
    Future(Right(MessageProcessorReturnValue(itemNeedsArchiving.asJson, Seq(RMQDestination("vidispine-events", "vidispine.itemneedsarchive.nearline"))))) 
  }


  private def findPendingDeletionRecord(archivedRecord: ArchivedRecord): Future[Option[PendingDeletionRecord]] = {
    (for {
      byOriginal <- pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(archivedRecord.originalFilePath, MediaTiers.NEARLINE)
      byUploaded <- pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(archivedRecord.uploadedPath, MediaTiers.NEARLINE)
      // byVsItemId <- pendingDeletionRecordDAO.findByOnlineIdForNEARLINE(archivedRecord.vidispineItemId // maybe?)
    } yield (byOriginal, byUploaded)).map {
      case (Some(o), _) => Some(o)
      case (_, Some(u)) => Some(u)
      case _ => None
    }
  }

}


