package helpers

import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import org.slf4j.LoggerFactory
import utils.Ensurer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class PendingDeletionHelper(implicit
                            ec: ExecutionContext,
                            pendingDeletionRecordDAO: PendingDeletionRecordDAO) {

  private val logger = LoggerFactory.getLogger(getClass)


  def removeDeletionPendingByMessage(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    (msg.mediaTier, msg.vidispineItemId, msg.nearlineId) match {
      case ("NEARLINE", _, Some(nearlineId)) =>
        pendingDeletionRecordDAO
          .findByNearlineIdForNEARLINE(nearlineId)
          .flatMap({
            // The number returned in the Right is the number of affected rows.
            // We always call this method when we delete an item, instead of first checking
            // if there exists a pending deletion => if none is found, that's not an
            // error, but obviously no rows are updated, hence Right(0) in that case.
            case Some(existingRecord) =>
              deleteExistingPendingDeletionRecord(existingRecord)
            case None =>
              Future(Right(0))
          })

      case ("NEARLINE", _, _) =>
        Future.failed(new RuntimeException("NEARLINE but no nearlineId"))

      case ("ONLINE", Some(vsItemId), _) =>
        pendingDeletionRecordDAO
          .findByOnlineIdForONLINE(vsItemId)
          .flatMap({
            case Some(existingRecord) =>
              deleteExistingPendingDeletionRecord(existingRecord)
            case None =>
              Future(Right(0))
          })

      case ("ONLINE", _, _) =>
        Future.failed(new RuntimeException("ONLINE but no vsItemId"))

      case (_, _, _) =>
        Future.failed(new RuntimeException("This should not happen!"))
    }

  private def deleteExistingPendingDeletionRecord(existingRecord: PendingDeletionRecord) =
    pendingDeletionRecordDAO.deleteRecord(existingRecord).map { i =>
      logger.debug(s"Deleted ${getInformativePendingDeletionString(existingRecord)}")
      Right(i)
    }.recover {
      case e: Exception =>
        logger.warn(s"Failed to delete ${getInformativePendingDeletionString(existingRecord)}. Cause: ${e.getMessage}")
        Left(s"Failed to delete ${getInformativePendingDeletionString(existingRecord)}. Cause: ${e.getMessage}")
    }

  private def getInformativePendingDeletionString(project: PendingDeletionRecord) = {
    val id = project.id.getOrElse("<missing rec ID>")
    val nearlineId = project.nearlineId.getOrElse("<missing nearline ID>")
    val vsItemId = project.vidispineItemId.getOrElse("<missing vsItem ID>")

    s"${project.mediaTier} pendingDeletionRecord with " +
      s"id $id, " +
      s"nearlineId $nearlineId, " +
      s"onlineId $vsItemId " +
      s"originalFilePath '${project.originalFilePath}', " +
      s"attempt ${project.attempt}"
  }

  def storeDeletionPending(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    msg.originalFilePath match {
      case Some(filePath) =>
        Try {
          MediaTiers.withName(msg.mediaTier)
        } match {
          case Success(mediaTier) =>
            val recordMaybeFut = mediaTier match {
              case MediaTiers.ONLINE =>
                pendingDeletionRecordDAO.findByOnlineIdForONLINE(Ensurer.validateMediaId(msg.vidispineItemId))
              case MediaTiers.NEARLINE =>
                pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(Ensurer.validateMediaId(msg.nearlineId))
            }
            recordMaybeFut.map({
              case Some(existingRecord) => existingRecord.copy(attempt = existingRecord.attempt + 1)
              case None =>
                PendingDeletionRecord(None, originalFilePath = filePath, nearlineId = msg.nearlineId, vidispineItemId = msg.vidispineItemId, mediaTier = mediaTier, attempt = 1)
            }).flatMap(rec => {
              pendingDeletionRecordDAO
                .writeRecord(rec)
                .map(recId => Right(recId))
            })
          case Failure(ex) =>
            logger.warn(s"Unexpected value for MediaTier: ${ex.getMessage}")
            Future.failed(new RuntimeException(s"Cannot store PendingDeletion, unexpected value for mediaTier: '${msg.mediaTier}"))
        }
      case None =>
        logger.warn(s"No filepath for ${msg.asJson}, no use storing a PendingDeletion; dropping message")
        Future.failed(new RuntimeException("Cannot store PendingDeletion record for item without filepath"))
    }

}
