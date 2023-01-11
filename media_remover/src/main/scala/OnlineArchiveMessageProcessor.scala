import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.messages.{VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecordDAO
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.plutocore.AssetFolderLookup
import com.om.mxs.client.japi.Vault
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.slf4j.LoggerFactory
import utils.Ensurer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class OnlineArchiveMessageProcessor(asLookup: AssetFolderLookup, selfHealRetryLimit:Int=10)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  nearlineRecordDAO: NearlineRecordDAO,
  ec: ExecutionContext,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
  s3ObjectChecker: S3ObjectChecker,
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
          case Left(err) => Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
              handleDeepArchiveCompleteForNearline(vault, archivedRecord)
            }
        }

      case "storagetier.onlinearchive.mediaingest.online" =>
        msg.as[ArchivedRecord] match {
          case Left(err) => Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
            handleDeepArchiveCompleteForOnline(archivedRecord)
        }

      case _ =>
        logger.warn(s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it.")
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }

  /**
   * Ruh-roh, Raggy! The archivedRecord does not contain a nearlineId, so we have to find the corresponding pending
   * deletion record(s) some other way.
   *<p><blockquote><pre>
   *  Pseudocode
   *  ----------
   *  Get a list of all PDRs - order by attempt - do we have to check both paths? only uploadedPath? Nah, only originalFilePath; neither uploadedPath nor vidispineItemId is needed
   *    if empty, SilentDrop
   *    if 1, happy days, regular check, using archivedRecord's uploadedPath
   *      if exists
   *        delete item
   *        delete pdr
   *      if not exists
   *        if pdr has vsItemId
   *          update pdr
   *          request archive
   *        else
   *          DLQ
   *    if > 1
   *      find first existing
   *        delete item
   *        delete pdr
   *      if no existing
   *        filter by presence of vsItemId
   *        if none left
   *          DLQ
   *        else
   *          update pdr for the first one
   *          request archive for the same one
   *</pre></blockquote></p>
   *
   * @param vault
   * @param archivedRecord
   * @return
   */
  def handleDeepArchiveCompleteForNearline(vault: Vault, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    // FIXME It seems we do not have nearlineId in an archiveRecord, and as we've previously noticed, originalFilePath can be the same for more than one nearline media item
    // TODO Alright, if we get a AR for a nearline item, fetch all nearline pending records for that originalFilePath.. and.. what.. check all?
    // Search for originalFilePath AND uploadedPath. If either of those matches, use the the found PendingDeletionRecord as source.
    // OR NOT. I think we need to use originalFilePath to find the correct PDR, but use uploadedPath when checking against S3...
    // WHICH should work fine for ArchivedRecord


    findPendingDeletionRecords(archivedRecord).flatMap(recs => recs.length match {
      case 0 =>
        logger.debug(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}")
        throw SilentDropMessage(Some(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}"))

      case 1 =>
        val rec = recs.head
        rec.nearlineId match {
          case Some(nearlineId) => getNearlineDataAndCheckExistsInDeepArchive(vault, archivedRecord, rec, nearlineId).flatMap({
            case true => handleNearlineDeletion(vault, rec)
            case false => retryRequireCopyOrFail(rec, nearlineId)
          }).recover({
            // We only want to transform S3 connection errors to Lefts
            case err: Throwable if !err.getMessage.startsWith("Cannot request deep archive copy") =>
              logger.info(s"Could not connect to deep archive to check if copy of ${MediaTiers.NEARLINE} media exists, do not delete. Reason: ${err.getMessage}")
              Left(s"Could not connect to deep archive to check if copy of ${MediaTiers.NEARLINE} media exists, do not delete. Reason: ${err.getMessage}")
          })
          case None =>
            logger.warn(s"No nearline ID in pending deletion rec for media ${archivedRecord.originalFilePath}")
            Future.failed(new RuntimeException(s"No nearline ID in pending deletion rec for media ${archivedRecord.originalFilePath}"))
        }

      case _ =>
        val recsWithNearlineId = recs.collect({ case rec if rec.nearlineId.isDefined => rec })
        if (recsWithNearlineId.isEmpty) {
          retryRequireCopyOrFail(archivedRecord, recs)
        } else {
          val listOfExistsRecTupleFuts = recsWithNearlineId.map(rec => getNearlineDataAndCheckExistsInDeepArchive(vault, archivedRecord, rec, rec.nearlineId.get).zip(Future(rec)))
          Future.sequence(listOfExistsRecTupleFuts).flatMap(_.filter(_._1) match {
            case listOfExistsRecTuples if listOfExistsRecTuples.nonEmpty => handleNearlineDeletion(vault, listOfExistsRecTuples.head)
            case _ => retryRequireCopyOrFail(archivedRecord, recs)
          }).recover({
            // We only want to transform S3 connection errors to Lefts
            case err: Throwable if !err.getMessage.startsWith("Cannot request deep archive copy") =>
              logger.info(s"Could not connect to deep archive to check if copy of ${MediaTiers.NEARLINE} media exists, do not delete. Reason: ${err.getMessage}")
              Left(s"Could not connect to deep archive to check if copy of ${MediaTiers.NEARLINE} media exists, do not delete. Reason: ${err.getMessage}")
          })
        }
    })
  }


  private def handleNearlineDeletion(vault: Vault, rec: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    for {
      result <- nearlineHelper.deleteMediaFromNearline(vault, rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, rec.vidispineItemId)
      _ <- pendingDeletionRecordDAO.deleteRecord(rec)
    } yield result
  }

  private def retryRequireCopyOrFail(rec: PendingDeletionRecord, nearlineId: String): Future[Either[String, MessageProcessorReturnValue]] = {
    rec.vidispineItemId match {
      case Some(vsItemId) =>
        if (rec.attempt >= selfHealRetryLimit) {
          Future.failed(new RuntimeException(s"Cannot request deep archive copy - too many self-heal retries for ${rec.mediaTier} ${rec.nearlineId.get}, ${rec.originalFilePath}, see logs for details"))
        } else {
          pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
          outputDeepArchiveCopyRequiredForNearline(vsItemId, rec.mediaTier.toString, Some(rec.originalFilePath))
        }
      case None => Future.failed(new RuntimeException(s"Cannot request deep archive copy for ${rec.mediaTier}, ${rec.originalFilePath}, $nearlineId - missing vidispine ID!"))
    }
  }

  private def retryRequireCopyOrFail(archivedRecord: ArchivedRecord, recs: Seq[PendingDeletionRecord]): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info("None found")
    // handle none exist, select a patsy and update the pdr + request copy
    val recMaybe = recs.find(rec => rec.vidispineItemId.isDefined)
    recMaybe match {
      case Some(rec) =>
        if (rec.attempt >= selfHealRetryLimit) {
          Future.failed(new RuntimeException(s"Cannot request deep archive copy - too many self-heal retries for ${rec.mediaTier} ${rec.nearlineId.get}, ${rec.originalFilePath}, see logs for details"))
        } else {
          pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
          outputDeepArchiveCopyRequiredForNearline(rec.vidispineItemId.get, MediaTiers.NEARLINE.toString, Some(archivedRecord.originalFilePath))
        }
      case None =>
        Future.failed(new RuntimeException(s"Cannot request deep archive copy for ${MediaTiers.NEARLINE}, ${archivedRecord.originalFilePath}, none of the matching pending deletion records has a vidispine ID!"))
    }
  }

  private def handleNearlineDeletion(vault: Vault, existsRecTuple: (Boolean, PendingDeletionRecord)) = {
    val rec = existsRecTuple._2
    for {
      result <- nearlineHelper.deleteMediaFromNearline(vault, rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, rec.vidispineItemId)
      _ <- pendingDeletionRecordDAO.deleteRecord(rec)
    } yield result
  }

  def getNearlineDataAndCheckExistsInDeepArchive(vault: Vault, archivedRec: ArchivedRecord, pendingRec: PendingDeletionRecord, nearlineId: String): Future[Boolean] =
    for {
      // We fetch the current size, because we don't know how old the message is
      nearlineFileSize <- nearlineHelper.getNearlineFileSize(vault, nearlineId)
      checksumMaybe <- nearlineHelper.getChecksumForNearline(vault, nearlineId)
      exists <- s3ObjectChecker.nearlineMediaExistsInDeepArchive(checksumMaybe, nearlineFileSize, pendingRec.originalFilePath, archivedRec.uploadedPath)
    } yield exists


  /**
   * Easy-peasy. If we have a vidispineItemId in the archivedRecord, we can use that to check for <=1 pending deletion
   * records.
   *
   * @param archivedRecord
   * @return
   */
  def handleDeepArchiveCompleteForOnline(archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
    archivedRecord.vidispineItemId match {
      case Some(vsItemId) =>
        pendingDeletionRecordDAO.findByOnlineIdForONLINE(vsItemId).flatMap({
          case Some(rec) =>
            onlineHelper.getOnlineSize(vsItemId).flatMap(sizeMaybe => {
              val (fileSize, originalFilePath, _) = Ensurer.validateNeededFields(sizeMaybe, Some(rec.originalFilePath), Some(vsItemId))
              onlineHelper.getMd5ChecksumForOnline(vsItemId).flatMap(checksumMaybe => {
                s3ObjectChecker.onlineMediaExistsInDeepArchive(checksumMaybe, fileSize, originalFilePath, archivedRecord.uploadedPath).flatMap({
                  case true =>
                    for {
                      mediaRemovedMsg <- onlineHelper.deleteMediaFromOnline(rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, Some(vsItemId))
                      _ <- pendingDeletionRecordDAO.deleteRecord(rec)
                    } yield mediaRemovedMsg
                  case false =>
                    if (rec.attempt >= selfHealRetryLimit) {
                      Future.failed(new RuntimeException(s"Cannot request deep archive copy - too many self-heal retries for ${rec.mediaTier} ${rec.nearlineId.get}, ${rec.originalFilePath}, see logs for details"))
                    } else {
                      pendingDeletionRecordDAO.updateAttemptCount(rec.id.get, rec.attempt + 1)
                      outputDeepArchiveCopyRequiredForOnline(rec.vidispineItemId.get, rec.mediaTier.toString, Some(rec.originalFilePath))
                    }
                })
              })
            }).recover({
                // We only want to transform S3 connection errors to Lefts
                case err: Throwable if !err.getMessage.startsWith("Cannot request deep archive copy") =>
                  logger.info(s"Could not connect to deep archive to check if copy of ${MediaTiers.ONLINE} media exists, do not delete. Reason: ${err.getMessage}")
                  Left(s"Could not connect to deep archive to check if copy of ${MediaTiers.ONLINE} media exists, do not delete. Reason: ${err.getMessage}")
              })

          case None =>
            Future.failed(SilentDropMessage(Some(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.ONLINE} item with ${archivedRecord.originalFilePath}")))
        })
      case None =>
        Future(Left(s"Although deep archive is complete for ${MediaTiers.ONLINE} item with ${archivedRecord.originalFilePath}, the archive record is missing the vsItemId which we need to do the deletion"))
    }

  // online DA
  def outputDeepArchiveCopyRequiredForOnline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = {
    // We know we have an online item to delete, but no DA
    // -> vidispine.itemneedsbackup ---> vidispine.itemneedsarchive because we don't wanna trigger the other copy needlessly
    // -> implement copy from vs a la online_nearline but to S3
    logger.info(s"Outputting Deep Archive copy required for online item (not removing $tier $path)")
    val itemNeedsArchiving = VidispineMediaIngested(List(VidispineField("itemId", itemId)))
    Future(Right(MessageProcessorReturnValue(itemNeedsArchiving.asJson, Seq(RMQDestination("vidispine-events", "vidispine.itemneedsarchive.online")))))
  }

  def outputDeepArchiveCopyRequiredForNearline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"Outputting Deep Archive copy required for nearline item (not removing $tier $path)")
    val itemNeedsArchiving = VidispineMediaIngested(List(VidispineField("itemId", itemId)))
    Future(Right(MessageProcessorReturnValue(itemNeedsArchiving.asJson, Seq(RMQDestination("vidispine-events", "vidispine.itemneedsarchive.nearline"))))) 
  }

  def findPendingDeletionRecords(archivedRecord: ArchivedRecord): Future[Seq[PendingDeletionRecord]] = {
    // TODO Possibly change to get also, or only, by vidispineItemId
    pendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(archivedRecord.originalFilePath)
  }

}


