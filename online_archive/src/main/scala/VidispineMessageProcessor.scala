import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import io.circe.Json
import io.circe.generic.auto._
import messages.VidispineMediaIngested
import org.slf4j.LoggerFactory
import plutocore.{AssetFolderLookup, PlutoCoreConfig}

import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class VidispineMessageProcessor(plutoCoreConfig: PlutoCoreConfig)
                               (implicit archivedRecordDAO: ArchivedRecordDAO,
                                failureRecordDAO: FailureRecordDAO,
                                ignoredRecordDAO: IgnoredRecordDAO,
                                ec: ExecutionContext,
                                mat: Materializer,
                                system: ActorSystem) extends MessageProcessor {
  private lazy val asLookup = new AssetFolderLookup(plutoCoreConfig)
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * assembles a java.nio.Path pointing to a file, catching exceptions and converting to an Either
   * @param filePath
   * @return
   */
  private def compositingGetPath(filePath: String) =
    Try {
      Paths.get(filePath)
    }.toEither.left.map(_.getMessage)

  /**
   * Upload ingested file if not already exist.
   *
   * @param filePath       path to the file that has been ingested
   * @param mediaIngested  the media object ingested by Vidispine
   *
   * @return String explaining which action took place
   */
  def uploadIfRequiredAndNotExists(filePath: String, mediaIngested: VidispineMediaIngested): Future[String] = {
    // 1. Check file already exists
    archivedRecordDAO.findBySourceFilename(filePath)
      .map(maybeRecord=>{
        (maybeRecord.map(_.originalFileSize), mediaIngested.fileSize) match {
          case (Some(originalFileSize), Some(ingestedFileSize))=>
            originalFileSize == ingestedFileSize
          case _=>
            false
        }
      }).flatMap({
        case true => Future("File already exist")
        case false =>
          // 2. Download full item metadata and upload to s3
          // 3. Push message to own exchange explaining what has happened
          Future("Not implemented yet!")
      })
  }

  /**
   * Verify status of the ingested media and return an Exception if status is failed
   * and continue to potentially upload the ingested media.
   *
   * @param mediaIngested  the media object ingested by Vidispine
   *
   * @return String explaining which action took place
   */
  def handleIngestedMedia(mediaIngested: VidispineMediaIngested) = {
    val status = mediaIngested.status
    val itemId = mediaIngested.itemId

    if (status.contains("FAILED") || itemId.isEmpty)
      Future.failed(new RuntimeException(s"Import status not in correct state for archive $status itemId=${itemId}"))
    else {
      mediaIngested.filePath match {
        case Some(filePath)=>
          getRelativePath(filePath) match {
            case Left(err) =>
              logger.error(s"Could not relativize file path $filePath: $err. Uploading to $filePath")
              uploadIfRequiredAndNotExists(filePath, mediaIngested).map(Left(_))
            case Right(relativePath) =>
              uploadIfRequiredAndNotExists(relativePath.toString, mediaIngested).map(Left(_))
          }
        case None=>
          Future(Left(s"File path for ingested media is missing $status itemId=${itemId}"))
      }
    }
  }

  def getRelativePath(filePath: String): Either[String, Path] = {
    compositingGetPath(filePath).flatMap(path => asLookup.relativizeFilePath(path))
  }

  /**
   * Override this method in your subclass to handle an incoming message
   *
   * @param routingKey the routing key of the message as received from the broker.
   * @param msg        the message body, as a circe Json object. You can unmarshal this into a case class by
   *                   using msg.as[CaseClassFormat]
   * @return You need to return Left() with a descriptive error string if the message could not be processed, or Right
   *         with a circe Json body (can be done with caseClassInstance.noSpaces) containing a message body to send
   *         to our exchange with details of the completed operation
   */
  def handleRawImportStop(msg: Json): Future[Either[String, Json]] = {
    msg.as[VidispineMediaIngested] match {
      case Left(err) =>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a VidispineMediaIngested: $err"))
      case Right(mediaIngested)=>
        handleIngestedMedia(mediaIngested)
    }
  }

  /**
   * Override this method in your subclass to handle an incoming message
   *
   * @param routingKey the routing key of the message as received from the broker.
   * @param msg        the message body, as a circe Json object. You can unmarshal this into a case class by
   *                   using msg.as[CaseClassFormat]
   * @return You need to return Left() with a descriptive error string if the message could not be processed, or Right
   *         with a circe Json body (can be done with caseClassInstance.noSpaces) containing a message body to send
   *         to our exchange with details of the completed operation
   */
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, Json]] = {
    routingKey match {
      case "vidispine.job.raw_import.stop"=>
        handleRawImportStop(msg)
      case _=>
        logger.warn(s"Dropping message $routingKey from vidispine exchange as I don't know how to handle it. This should be fixed in" +
          s" the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}