import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, ArchivedRecordRow, ErrorComponents, FailureRecord, FailureRecordDAO, FailureRecordRow, IgnoredRecord, IgnoredRecordDAO, IgnoredRecordRow, RetryStates}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import messages.VidispineMediaIngested
import org.slf4j.LoggerFactory
import plutocore.{AssetFolderLookup, PlutoCoreConfig}

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try}

class VidispineMessageProcessor(plutoCoreConfig: PlutoCoreConfig)
                               (implicit archivedRecordDAO: ArchivedRecordDAO,
                                failureRecordDAO: FailureRecordDAO,
                                ignoredRecordDAO: IgnoredRecordDAO,
                                ec: ExecutionContext,
                                mat: Materializer,
                                system: ActorSystem,
                                uploader: FileUploader) extends MessageProcessor {
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

  private def uploadCreateOrUpdateRecord(filePath:String, relativePath:String, archivedRecord: Option[ArchivedRecordRow#TableElementType]) = {
    logger.info(s"Archiving file '$filePath' to s3://${uploader.bucketName}/$relativePath")
    Future.fromTry(
      uploader.copyFileToS3(new File(filePath), Some(relativePath))
    ).flatMap(fileInfo => {
      val (fileName, fileSize) = fileInfo
      logger.debug(s"$filePath: Upload completed")
      val record = archivedRecord match {
        case Some(rec) =>
          logger.debug(s"archivehunter ID for $relativePath is ${rec.archiveHunterID}")
          rec.copy(
            originalFileSize = fileSize,
            uploadedPath = fileName,
            //FIXME: Unsure if anything else needs to be updated here. Vidispine ID perhaps?
          )
        case None =>
          val archiveHunterID = utils.ArchiveHunter.makeDocId(bucket = uploader.bucketName, fileName)
          logger.debug(s"archivehunter ID for $relativePath is $archiveHunterID")
          ArchivedRecord(archiveHunterID,
            originalFilePath=filePath,
            originalFileSize=fileSize,
            uploadedBucket = uploader.bucketName,
            uploadedPath = fileName,
            uploadedVersion = None)
      }

      archivedRecordDAO
        .writeRecord(record)
        .map(recId=>Right(record.copy(id=Some(recId)).asJson))
    }).recoverWith(err=>{
      val rec = FailureRecord(id = None,
        originalFilePath = filePath, // FIXME: Should this be archivedRecord.originalPath or the filePath?
        attempt = 1,  //FIXME: need to be passed the retry number by the Framework
        errorMessage = err.getMessage,
        errorComponent = ErrorComponents.Internal,
        retryState = RetryStates.WillRetry)
      failureRecordDAO.writeRecord(rec).map(_=>Left(err.getMessage))
    })
  }

  /**
   * Upload ingested file if not already exist.
   *
   * @param filePath       path to the file that has been ingested
   * @param mediaIngested  the media object ingested by Vidispine
   *
   * @return String explaining which action took place
   */
  def uploadIfRequiredAndNotExists(filePath: String,
                                   relativePath: String,
                                   mediaIngested: VidispineMediaIngested): Future[Either[String, Json]] = {
    for {
      maybeArchivedRecord <- archivedRecordDAO.findBySourceFilename(filePath)
      maybeIgnoredRecord <- ignoredRecordDAO.findBySourceFilename(filePath)
      maybeFailureRecord <- failureRecordDAO.findBySourceFilename(filePath)
      result <- (maybeIgnoredRecord, maybeArchivedRecord) match {
        case (Some(_), _) => Future(Left("Record should be ignored"))
        case (None, Some(archivedRecord)) =>
          if(maybeFailureRecord.isDefined) logger.warn("This job failed previously")
          if(archivedRecord.archiveHunterID.isEmpty || !archivedRecord.archiveHunterIDValidated) {
            logger.info(s"Archive hunter ID does not exist, will retry")
            Future(Left("Archive hunter ID does not exist yet, will retry"))
          } else Future.fromTry(uploader.objectExists(archivedRecord.uploadedPath))
            .flatMap(exist => {
              if (exist) {
                logger.info("Filepath in record already exists in S3 bucket")
                Future(Right(archivedRecord.asJson))
              } else {
                logger.warn("Does not exist in S3, re-uploading")
                uploadCreateOrUpdateRecord(filePath, relativePath, Some(archivedRecord))
              }
            })
        case (None, None) =>
          if(maybeFailureRecord.isDefined) {
            logger.warn("This job failed previously")
          }

          uploadCreateOrUpdateRecord(filePath, relativePath, None)
      }
    } yield result
  }

  /**
   * Verify status of the ingested media and return an Exception if status is failed
   * and continue to potentially upload the ingested media.
   *
   * @param mediaIngested  the media object ingested by Vidispine
   *
   * @return String explaining which action took place
   */
  def handleIngestedMedia(mediaIngested: VidispineMediaIngested): Future[Either[String, Json]] = {
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
              uploadIfRequiredAndNotExists(filePath, filePath, mediaIngested)
            case Right(relativePath) =>
              uploadIfRequiredAndNotExists(filePath, relativePath.toString, mediaIngested)
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