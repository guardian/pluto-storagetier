import VidispineFunctions.uploadKeyForProxy
import akka.stream.Materializer
import archivehunter.ArchiveHunterCommunicator
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.VidispineMediaIngested
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecord, FailureRecordDAO, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.utils.FilenameSplitter
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VSShapeFile, VidispineCommunicator}
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import org.slf4j.{LoggerFactory, MDC}
import utils.ArchiveHunter

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object VidispineFunctions {
  private val logger = LoggerFactory.getLogger(getClass)
  /**
   * determines an appropriate S3 key to use for the proxy of the given file
   * @param archivedRecord ArchivedRecord representing the "original" media for this content
   * @param proxyFile VSShapeFile object representing the File portion that Vidispine returned
   * @return a String containing the path to upload to
   */
  def uploadKeyForProxy(archivedRecord: ArchivedRecord, proxyFile:VSShapeFile) = {
    val uploadedPath = Paths.get(archivedRecord.uploadedPath)

    val proxyFileParts = proxyFile.uri.flatMap(_.headOption.flatMap(_.split("/").lastOption)) match {
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

class VidispineFunctions(mediaFileUploader:FileUploader, proxyFileUploader:FileUploader)
                        (implicit archivedRecordDAO: ArchivedRecordDAO,
                         failureRecordDAO: FailureRecordDAO,
                         ignoredRecordDAO: IgnoredRecordDAO,
                         ec:ExecutionContext,
                         mat:Materializer,
                         vidispineCommunicator: VidispineCommunicator,
                         archiveHunterCommunicator: ArchiveHunterCommunicator) {
  private val logger = LoggerFactory.getLogger(getClass)

  def proxyBucketName = proxyFileUploader.bucketName
  def mediaBucketName = mediaFileUploader.bucketName

  /**
   * The retry attempt number should be set in the message diagnostic context (MDC) by the framework.
   * Call this method to get the number
   * @return an Option containing the retry count number, or None if there was an error / it was not set
   */
  def attemptCountFromMDC() = {
    val attemptCount = for {
      maybeStringValue <- Try { Option(MDC.get("retryAttempt")) }
      maybeIntValue <- Try { maybeStringValue.map(_.toInt) }
    } yield maybeIntValue
    attemptCount.toOption.flatten
  }

  private def uploadCreateOrUpdateRecord(filePath:String, relativePath:String, mediaIngested: VidispineMediaIngested,
                                         archivedRecord: Option[ArchivedRecord]) = {
    logger.info(s"Archiving file '$filePath' to s3://${mediaFileUploader.bucketName}/$relativePath")

    mediaFileUploader.copyFileToS3(new File(filePath), Some(relativePath)).flatMap(fileInfo => {
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
      val attemptCount = attemptCountFromMDC() match {
        case Some(count)=>count
        case None=>
          logger.warn(s"Could not get attempt count from diagnostic context for $filePath")
          1
      }
      val rec = FailureRecord(id = None,
        originalFilePath = archivedRecord.map(_.originalFilePath).getOrElse(filePath),
        attempt = attemptCount,
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
   * check if a file exists. It's put into its own method so it can be over-ridden in tests
   * @param filePath path to check
   * @return boolean indicating if it exists
   */
  protected def internalCheckFile(filePath:Path) = Files.exists(filePath)

  /**
   * uploads the proxy shape for the given item to S3 using the provided proxyFileUploader
   * @param fileInfo VSShapeFile representing the proxy file
   * @param archivedRecord ArchivedRecord representing the item that needs to get
   * @param shapeDoc ShapeDocument representing the proxy's shape
   * @return a Future, with a tuple of the uploaded filename and file size. On error, the future will fail.
   */
  private def doUploadShape(fileInfo:VSShapeFile, archivedRecord: ArchivedRecord, shapeDoc:ShapeDocument) = {
    val uploadKey = VidispineFunctions.uploadKeyForProxy(archivedRecord, fileInfo)

    def filePathsForShape(f:VSShapeFile) = f.uri match {
      case None=>Seq()
      case Some(uriList)=>
        uriList.map(u=>Try { URI.create(u) }.toOption).collect({case Some(uri)=>uri})
    }

    /**
     * checks the paths in the list using `internalCheckFile` and returns the first one that passes the check, or None
     * if there are no results
     * @param paths a list of paths to check
     * @return either a checked, working Path or None
     */
    def firstCheckedFilepath(paths:Seq[Path]) = paths.foldLeft[Option[Path]](None)((result, filePath)=>{
      result match {
        case existingResult@Some(_)=>existingResult
        case None=>
          if (internalCheckFile(filePath)) {
            logger.info(s"Found filePath for Vidispine shape $filePath")
            Some(filePath)
          } else {
            result
          }
      }
    })
    shapeDoc.getLikelyFile match {
      case None =>
        logger.error(s"${fileInfo} is a placeholder or has no original media")
        Future.failed(new RuntimeException(s"${fileInfo} is a placeholder or has no original media"))
      case Some(fileInfo) =>
        val uriList = filePathsForShape(fileInfo)
        if(uriList.isEmpty) {
          logger.error(s"Either ${fileInfo.uri} is empty or it does not contain a valid URI")
          Future.failed(new RuntimeException(s"Fileinfo $fileInfo has no valid URI"))
        } else {
          val filePaths = uriList.map(Paths.get)
          firstCheckedFilepath(filePaths) match {
            case Some(filePath)=>
              logger.info(s"Starting upload of $filePath to s3://${proxyFileUploader.bucketName}/$uploadKey")
              proxyFileUploader.copyFileToS3(filePath.toFile, Some(uploadKey))
            case None=>
              logger.error(s"Could not find path for URI ${fileInfo.uri} on-disk")
              Future.failed(new RuntimeException(s"File $fileInfo could not be found"))
          }
        }
    }
  }

  def uploadShapeIfRequired(itemId: String, shapeId: String, shapeTag:String, archivedRecord: ArchivedRecord):Future[Either[String,MessageProcessorReturnValue]] = {
    ArchiveHunter.shapeTagToProxyTypeMap.get(shapeTag) match {
      case None=>
        logger.info(s"Shape $shapeTag for item $itemId is not required for ArchiveHunter, dropping the message")
        Future.failed(SilentDropMessage())
      case Some(destinationProxyType)=>
        vidispineCommunicator.findItemShape(itemId, shapeId).flatMap({
          case None=>
            logger.error(s"Shape $shapeId does not exist on item $itemId despite a notification informing us that it does.")
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

  def uploadThumbnailsIfRequired(itemId:String, essenceVersion:Option[Int], archivedRecord: ArchivedRecord):Future[Either[String, Unit]] = {
    vidispineCommunicator.akkaStreamFirstThumbnail(itemId, essenceVersion).flatMap({
      case None=>
        logger.error(s"No thumbnail is available for item $itemId ${if(essenceVersion.isDefined) " with essence version "+essenceVersion.get}")
        Future(Right( () ))
      case Some(streamSource)=>
        logger.info(s"Uploading thumbnail for $itemId at version $essenceVersion - content type is ${streamSource.contentType}, content length is ${streamSource.contentLengthOption.map(_.toString).getOrElse("not set")}")
        val uploadedPathXtn = FilenameSplitter(archivedRecord.uploadedPath)
        val thumbnailPath = uploadedPathXtn._1 + "_thumb.jpg"
        val result = for {
          uploadResult <- proxyFileUploader.uploadAkkaStream(streamSource.dataBytes, thumbnailPath, streamSource.contentType, streamSource.contentLengthOption, allowOverwrite = true)
          _ <- archiveHunterCommunicator.importProxy(archivedRecord.archiveHunterID, uploadResult.key, uploadResult.bucket, ArchiveHunter.ProxyType.THUMBNAIL)
        } yield uploadResult.location
        result.map(_=>Right( () ) ) //throw away the final result, we just need to know it worked.
    })
  }

  def uploadMetadataToS3(itemId: String, essenceVersion: Option[Int], archivedRecord: ArchivedRecord): Future[Either[String, Json]] = {
    vidispineCommunicator.akkaStreamXMLMetadataDocument(itemId).flatMap({
      case None=>
        logger.error(s"No metadata present on $itemId")
        Future.failed(new RuntimeException(s"No metadata present on $itemId"))
      case Some(entity)=>
        logger.info(s"Got metadata source from Vidispine: ${entity} uploading to S3 bucket")
        val uploadedPathXtn = FilenameSplitter(archivedRecord.uploadedPath)
        val metadataPath = uploadedPathXtn._1 + "_metadata.xml"

        for {
          uploadResult <- proxyFileUploader.uploadAkkaStream(entity.dataBytes, metadataPath, entity.contentType, entity.contentLengthOption, allowOverwrite = true)
          _ <- archiveHunterCommunicator.importProxy(archivedRecord.archiveHunterID, uploadResult.key, uploadResult.bucket, ArchiveHunter.ProxyType.METADATA)
          updatedRecord <- Future(archivedRecord.copy(
            proxyBucket = Some(uploadResult.bucket),
            metadataXML = Some(uploadResult.key),
            metadataVersion = essenceVersion
          ))
          _ <- archivedRecordDAO.writeRecord(updatedRecord)
        } yield Right(updatedRecord.asJson)
    }).recoverWith(err=>{
      val attemptCount = attemptCountFromMDC() match {
        case Some(count)=>count
        case None=>
          logger.warn(s"Could not get attempt count from diagnostic context for $itemId")
          1
      }

      val rec = FailureRecord(id = None,
        originalFilePath = archivedRecord.originalFilePath,
        attempt = attemptCount,
        errorMessage = err.getMessage,
        errorComponent = ErrorComponents.AWS,
        retryState = RetryStates.WillRetry)
      failureRecordDAO.writeRecord(rec).map(_=>Left(err.getMessage))
    })
  }
}
