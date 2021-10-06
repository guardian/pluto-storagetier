import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.ArchiveHunterCommunicator
import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, ErrorComponents, FailureRecord, FailureRecordDAO, IgnoredRecordDAO, RetryStates}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.utils.FilenameSplitter
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VSShapeFile, VidispineCommunicator}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.VidispineMediaIngested
import org.slf4j.LoggerFactory
import plutocore.{AssetFolderLookup, PlutoCoreConfig}
import utils.ArchiveHunter

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

object VidispineMessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * assembles a java.nio.Path pointing to a file, catching exceptions and converting to an Either
   * @param filePath string representing the file path
   * @return either the Path, or a Left containing an error string.
   */
  private def compositingGetPath(filePath: String) =
    Try {
      Paths.get(filePath)
    }.toEither.left.map(_.getMessage)

  /**
   * determines an appropriate S3 key to use for the proxy of the given file
   * @param archivedRecord ArchivedRecord representing the "original" media for this content
   * @param proxyFile VSShapeFile object representing the File portion that Vidispine returned
   * @return a String containing the path to upload to
   */
  def uploadKeyForProxy(archivedRecord: ArchivedRecord, proxyFile:VSShapeFile) = {
    val uploadedPath = Paths.get(archivedRecord.uploadedPath)

    val proxyFileParts = proxyFile.uri.headOption.flatMap(_.split("/").lastOption) match {
      case None=>
        logger.error("No proxy file URI in information? This is unexpected.")
        ("", None)
      case Some(proxyFileName)=>
        FilenameSplitter(proxyFileName)
    }

    val uploadedFileName = FilenameSplitter(uploadedPath.getFileName.toString)

    uploadedPath.getParent.toString + "/" + uploadedFileName._1 + "_prox" + proxyFileParts._2.getOrElse("")
  }

}

class VidispineMessageProcessor(plutoCoreConfig: PlutoCoreConfig,
                                mediaFileUploader:FileUploader,
                                proxyFileUploader: FileUploader)
                               (implicit archivedRecordDAO: ArchivedRecordDAO,
                                failureRecordDAO: FailureRecordDAO,
                                ignoredRecordDAO: IgnoredRecordDAO,
                                vidispineCommunicator: VidispineCommunicator,
                                archiveHunterCommunicator: ArchiveHunterCommunicator,
                                ec: ExecutionContext,
                                mat: Materializer,
                                system: ActorSystem) extends MessageProcessor {
  private lazy val asLookup = new AssetFolderLookup(plutoCoreConfig)
  private val logger = LoggerFactory.getLogger(getClass)
  import VidispineMessageProcessor._

  def getRelativePath(filePath: String): Either[String, Path] = {
    compositingGetPath(filePath).flatMap(path => asLookup.relativizeFilePath(path))
  }

  private def uploadCreateOrUpdateRecord(filePath:String, relativePath:String, mediaIngested: VidispineMediaIngested,
                                         archivedRecord: Option[ArchivedRecord]) = {
    logger.info(s"Archiving file '$filePath' to s3://${mediaFileUploader.bucketName}/$relativePath")
    Future.fromTry(
      mediaFileUploader.copyFileToS3(new File(filePath), Some(relativePath))
    ).flatMap(fileInfo => {
      val (fileName, fileSize) = fileInfo
      logger.debug(s"$filePath: Upload completed")
      val record = archivedRecord match {
        case Some(rec) =>
          logger.debug(s"actual archivehunter ID for $relativePath is ${rec.archiveHunterID}")
          rec.copy(
            originalFileSize = fileSize,
            uploadedPath = fileName,
            vidispineItemId = mediaIngested.itemId,
            vidispineVersionId = mediaIngested.essenceVersion
          )
        case None =>
          val archiveHunterID = utils.ArchiveHunter.makeDocId(bucket = mediaFileUploader.bucketName, fileName)
          logger.debug(s"Provisional archivehunter ID for $relativePath is $archiveHunterID")
          ArchivedRecord(None,
            archiveHunterID,
            archiveHunterIDValidated=false,
            originalFilePath=filePath,
            originalFileSize=fileSize,
            uploadedBucket = mediaFileUploader.bucketName,
            uploadedPath = fileName,
            uploadedVersion = None,
            vidispineItemId = mediaIngested.itemId,
            vidispineVersionId = mediaIngested.essenceVersion,
            None,
            None,
            None,
            None,
            None
          )
      }

      logger.info(s"Updating record for ${record.originalFilePath} with vidispine ID ${mediaIngested.itemId}, vidispine version ${mediaIngested.essenceVersion}. ${if(!record.archiveHunterIDValidated) "ArchiveHunter ID validation is required."}")
      archivedRecordDAO
        .writeRecord(record)
        .map(recId=>Right(record.copy(id=Some(recId)).asJson))
    }).recoverWith(err=>{
      val rec = FailureRecord(id = None,
        originalFilePath = archivedRecord.map(_.originalFilePath).getOrElse(filePath),
        attempt = 1,  //FIXME: need to be passed the retry number by the Framework
        errorMessage = err.getMessage,
        errorComponent = ErrorComponents.AWS,
        retryState = RetryStates.WillRetry)
      failureRecordDAO.writeRecord(rec).map(_=>Left(err.getMessage))
    })
  }

  private def showPreviousFailure(maybeFailureRecord:Option[FailureRecord], filePath: String) = {
    if (maybeFailureRecord.isDefined) {
      val reason = maybeFailureRecord.map(rec => rec.errorMessage)
      logger.warn(s"This job with filepath $filePath failed previously with reason $reason")
    }
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
    logger.debug(s"uploadIfRequiredAndNotExists: Original file is $filePath, target path is $relativePath")
    for {
      maybeArchivedRecord <- archivedRecordDAO.findBySourceFilename(filePath)
      maybeIgnoredRecord <- ignoredRecordDAO.findBySourceFilename(filePath)
      maybeFailureRecord <- failureRecordDAO.findBySourceFilename(filePath)
      result <- (maybeIgnoredRecord, maybeArchivedRecord) match {
        case (Some(ignoreRecord), _) =>
          Future(Left(s"${filePath} should be ignored due to reason ${ignoreRecord.ignoreReason}"))
        case (None, Some(archivedRecord)) =>
          showPreviousFailure(maybeFailureRecord, filePath)

          if(archivedRecord.archiveHunterID.isEmpty || !archivedRecord.archiveHunterIDValidated) {
            logger.info(s"Archive hunter ID does not exist yet for filePath $filePath, will retry")
            Future(Left(s"Archive hunter ID does not exist yet for filePath $filePath, will retry"))
          } else Future.fromTry(mediaFileUploader.objectExists(archivedRecord.uploadedPath))
            .flatMap(exist => {
              if (exist) {
                logger.info(s"Filepath $filePath already exists in S3 bucket")
                val record = archivedRecord.copy(
                  vidispineItemId = mediaIngested.itemId,
                  vidispineVersionId = mediaIngested.essenceVersion
                )

                logger.info(s"Updating record for ${record.originalFilePath} with vidispine ID ${mediaIngested.itemId} and version ${mediaIngested.essenceVersion}")
                archivedRecordDAO
                  .writeRecord(record)
                  .map(recId=>Right(record.copy(id=Some(recId)).asJson))
              } else {
                logger.warn(s"Filepath $filePath does not exist in S3, re-uploading")
                uploadCreateOrUpdateRecord(filePath, relativePath, mediaIngested, Some(archivedRecord))
              }
            })
        case (None, None) =>
          showPreviousFailure(maybeFailureRecord, filePath)
          uploadCreateOrUpdateRecord(filePath, relativePath, mediaIngested, None)
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

    logger.debug(s"Received message content $mediaIngested")
    if (status.contains("FAILED") || itemId.isEmpty) {
      logger.error(s"Import status not in correct state for archive $status itemId=${itemId}")
      Future.failed(new RuntimeException(s"Import status not in correct state for archive $status itemId=${itemId}"))
    } else {
      mediaIngested.sourceFileId match {
        case Some(fileId)=>
          logger.debug(s"Got ingested file ID $fileId from the message")
          for {
            absPath <- vidispineCommunicator.getFileInformation(fileId).map(_.flatMap(_.getAbsolutePath))
            result <- absPath match {
              case None=>
                logger.error(s"Could not get absolute filepath for file $fileId")
                Future.failed(new RuntimeException(s"Could not get absolute filepath for file $fileId"))
              case Some(absPath)=>
                getRelativePath(absPath) match {
                  case Left(err) =>
                    logger.error(s"Could not relativize file path $absPath: $err. Uploading to $absPath")
                    uploadIfRequiredAndNotExists(absPath, absPath, mediaIngested)
                  case Right(relativePath) =>
                    uploadIfRequiredAndNotExists(absPath, relativePath.toString, mediaIngested)
                }
            }
          } yield result
        case None=>
          logger.error(s"The incoming message had no source file ID parameter, can't continue")
          Future.failed(new RuntimeException(s"No source file ID parameter"))
      }
    }
  }

  /**
   *
   * @param msg        the message body, as a circe Json object. You can unmarshal this into a case class by
   *                   using msg.as[CaseClassFormat]
   * @return You need to return Left() with a descriptive error string if the message could not be processed, or Right
   *         with a circe Json body (can be done with caseClassInstance.noSpaces) containing a message body to send
   *         to our exchange with details of the completed operation
   */
  def handleRawImportStop(msg: Json): Future[Either[String, Json]] = {
    msg.as[VidispineMediaIngested] match {
      case Left(err) =>
        logger.error(s"Could not unmarshal vidispine.job.raw_import.stop message: $err")
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a VidispineMediaIngested: $err"))
      case Right(mediaIngested) =>
        handleIngestedMedia(mediaIngested)
    }
  }

  /**
   * uploads the proxy shape for the given item to S3 using the provided proxyFileUploader
   * @param fileInfo VSShapeFile representing the proxy file
   * @param archivedRecord ArchivedRecord representing the item that needs to get
   * @param shapeDoc ShapeDocument representing the proxy's shape
   * @return a Future, with a tuple of the uploaded filename and file size. On error, the future will fail.
   */
  private def doUploadShape(fileInfo:VSShapeFile, archivedRecord: ArchivedRecord, shapeDoc:ShapeDocument) = {
    val uploadKey = uploadKeyForProxy(archivedRecord, fileInfo)
    logger.info(s"Starting upload of ${fileInfo.uri.headOption} to s3://${proxyFileUploader.bucketName}/$uploadKey")
    for {
      inputStream <- vidispineCommunicator.streamFileContent(fileInfo.id)
      result <- Future.fromTry(proxyFileUploader.uploadStreamNoChecks(
        inputStream,
        uploadKey,
        shapeDoc.mimeType.headOption.getOrElse("application/octet-stream"),
        fileInfo.sizeOption,
        fileInfo.hash
      ))
    } yield result
  }

  def uploadShapeIfRequired(itemId: String, shapeId: String, shapeTag:String, archivedRecord: ArchivedRecord):Future[Either[String,Json]] = {
    ArchiveHunter.shapeTagToProxyTypeMap.get(shapeTag) match {
      case None=>
        val ignoreRecord = IgnoredRecord(None,"",s"Shape tag $shapeTag is not known to ArchiveHunter", Some(itemId), None)
        Future(Right(ignoreRecord.asJson))
      case Some(destinationProxyType)=>
        vidispineCommunicator.findItemShape(itemId, shapeId).flatMap({
          case None=>
            logger.error(s"Shape $shapeId does not exist on item $itemId despite a notification informing us of this.")
            Future.failed(new RuntimeException(s"Shape $shapeId does not exist"))
          case Some(shapeDoc)=>
            shapeDoc.getLikelyFile match {
              case None =>
                Future(Left(s"No file exists on shape $shapeId for item $itemId yet"))
              case Some(fileInfo) =>
                val uploadedFut = for {
                  uploadResult <- doUploadShape(fileInfo, archivedRecord, shapeDoc)
                  _ <- archiveHunterCommunicator.importProxy(archivedRecord.archiveHunterID, uploadResult._1, proxyFileUploader.bucketName, destinationProxyType)
                  updatedRecord <- Future(archivedRecord.copy(proxyBucket = Some(proxyFileUploader.bucketName), proxyPath = Some(uploadResult._1)))
                  _ <- archivedRecordDAO.writeRecord(updatedRecord)
                } yield Right(updatedRecord.asJson)

                //the future will fail if we can't upload to S3, but treat this as a retryable failure
                uploadedFut.recover({
                  case err:Throwable=>
                    logger.error(s"Could not upload ${fileInfo.uri} to S3: ${err.getMessage}", err)
                    Left(s"Could not upload ${fileInfo.uri} to S3")
                })
            }
          })
    }
  }

  def handleShapeUpdate(shapeId:String, shapeTag:String, itemId:String): Future[Either[String, Json]] = {
    for {
      maybeArchivedItem <- archivedRecordDAO.findByVidispineId(itemId)
      maybeIgnoredItem <- ignoredRecordDAO.findByVidispineId(itemId)
      result <- (maybeArchivedItem, maybeIgnoredItem) match {
        case (_, Some(ignoredItem))=>
          logger.info(s"Item $itemId is ignored because ${ignoredItem.ignoreReason}, leaving alone")
          Future(Right(ignoredItem.asJson))
        case (Some(archivedItem), None)=>
          if(archivedItem.archiveHunterIDValidated) {
            uploadShapeIfRequired(itemId, shapeId, shapeTag, archivedItem)
          } else {
            logger.info(s"ArchiveHunter ID for ${archivedItem.originalFilePath} has not been validated yet")
            Future(Left(s"ArchiveHunter ID for ${archivedItem.originalFilePath} has not been validated yet"))
          }
        case (None, None)=>
          logger.info(s"No record of vidispine item $itemId")
          Future(Left(s"No record of vidispine item $itemId"))
      }
    } yield result
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
    logger.info(s"Received message from vidispine with routing key $routingKey")
    (msg.as[VidispineMediaIngested], routingKey) match {
      case (Left(err), _) =>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a VidispineMediaIngested: $err"))
      case (Right(mediaIngested), "vidispine.job.raw_import.stop")=>
        handleIngestedMedia(mediaIngested)
      case (Right(shapeUpdate), "vidispine.item.shape.modify")=>
        (shapeUpdate.shapeId, shapeUpdate.shapeTag, shapeUpdate.itemId) match {
          case (None, _, _)=>
            logger.error("Shape update without any shape ID?")
            Future.failed(new RuntimeException(s"Received shape update ${msg.noSpaces} without any shapeId"))
          case (_, None, _)=>
            logger.error("Shape update without any shape tag?")
            Future.failed(new RuntimeException(s"Received shape update ${msg.noSpaces} without any shapeTag"))
          case (_, _, None)=>
            logger.error("Shape update without any item ID")
            Future.failed(new RuntimeException(s"Received shape update ${msg.noSpaces} without any itemId"))
          case (Some(shapeId), Some(shapeTag), Some(itemId))=>
            handleShapeUpdate(shapeId, shapeTag, itemId)
        }
      case (_, _)=>
        logger.warn(s"Dropping message $routingKey from vidispine exchange as I don't know how to handle it. This should be fixed in" +
          s" the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
