package helpers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.media_remover.PendingDeletionRecord
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VidispineCommunicator}
import io.circe.generic.auto._
import io.circe.syntax._
import messages.MediaRemovedMessage
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class OnlineHelper(implicit
                   ec: ExecutionContext,
                   mat: Materializer,
                   system: ActorSystem,
                   vidispineCommunicator: VidispineCommunicator,
                  ) {

  private val logger = LoggerFactory.getLogger(getClass)

  def getMd5ChecksumForOnline(vsItemId: String): Future[Option[String]] =
    getOriginalShapeForVidispineItem(vsItemId).map {
      case Left(err) =>
        logger.info(s"Couldn't get MD5 checksum for vsItemId $vsItemId: $err")
        None
      case Right(originalShape) => originalShape.getLikelyFile.flatMap(_.md5Option)
    }

  def getOnlineSize(itemId: String): Future[Option[Long]] =
    getVidispineSize(itemId).map {
      case Left(err) => throw new RuntimeException(s"no size, because: $err")
      case Right(sizeMaybe) => sizeMaybe
    }

  def getVidispineSize(itemId: String): Future[Either[String, Option[Long]]] =
    getOriginalShapeForVidispineItem(itemId).map {
      case Left(value) => Left(value)
      case Right(originalShape) => Right(originalShape.getLikelyFile.flatMap(_.sizeOption))
    }

  def getOriginalShapeForVidispineItem(itemId: String): Future[Either[String, ShapeDocument]] =
    vidispineCommunicator.listItemShapes(itemId).map {
      case None =>
        logger.error(s"Can't get original shape for vidispine item $itemId as it has no shapes on it")
        Left(s"Can't get original shape for vidispine item $itemId as it has no shapes on it")
      case Some(shapes) =>
        shapes.find(_.tag.contains("original")) match {
          case None =>
            logger.error(s"Can't get original shape for of vidispine item $itemId. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
            Left(s"Can't get original shape for of vidispine item $itemId. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
          case Some(originalShape: ShapeDocument) =>
            Right(originalShape)
        }
    }

  def deleteMediaFromOnline(rec: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] =
    deleteMediaFromOnline(rec.mediaTier.toString, Some(rec.originalFilePath), rec.nearlineId, rec.vidispineItemId)


  def deleteMediaFromOnline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] =
    deleteMediaFromOnline(onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId, onlineOutputMessage.vidispineItemId)


  def deleteMediaFromOnline(mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]): Future[Either[String, MessageProcessorReturnValue]] =
    (mediaTier, vidispineItemIdMaybe) match {
      case ("ONLINE", Some(vsItemId)) =>
        vidispineCommunicator.deleteItem(vsItemId)
          .map(_ => {
            logger.info(s"Deleted $mediaTier media: itemId $vsItemId, path ${filePathMaybe.getOrElse("<missing file path>")}")
            Right(MediaRemovedMessage(mediaTier, filePathMaybe.getOrElse("<missing file path>"), vidispineItemIdMaybe, nearlineIdMaybe).asJson)
          })
          .recover({
            case err: Throwable =>
              logger.warn(s"Failed to remove $mediaTier media: itemId $vsItemId, path ${filePathMaybe.getOrElse("<missing file path>")}, reason: ${err.getMessage}")
              Left(s"Failed to remove online media itemId $vsItemId, path ${filePathMaybe.getOrElse("<missing file path>")}, reason: ${err.getMessage}")
          })
      case (_, _) => throw new RuntimeException(s"Cannot delete from online, wrong media tier ($mediaTier), or missing item id (${vidispineItemIdMaybe.getOrElse("<missing item id>")})")
    }

}
