import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.ArchiveHunterCommunicator
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.VidispineMediaIngested
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecord, FailureRecordDAO, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.utils.FilenameSplitter
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VSShapeFile, VidispineCommunicator}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.slf4j.LoggerFactory
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig}
import plutodeliverables.PlutoDeliverablesConfig
import utils.ArchiveHunter

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
                                deliverablesConfig: PlutoDeliverablesConfig,
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

  def getRelativePath(filePath: String, maybeImportSource:Option[String]): Either[String, Path] = {
    compositingGetPath(filePath).flatMap(path =>{
      if(maybeImportSource.contains("pluto-deliverables")) {
        //this key is always set on material that was ingested via Deliverables.
        //We need to apply different naming rules to those; a base path taken from the configuration and then a specific number of subpath components
        logger.debug(s"File path $filePath came from pluto-deliverables")
        val result = Try {
          Paths.get(deliverablesConfig.uploadBasePath).resolve(
            path.subpath(path.getNameCount-deliverablesConfig.presevePathParts,path.getNameCount)
          )
        }.toEither.left.map(_.getMessage)
        logger.debug(s"Relative path result is $result")
        result
      } else {
        //if it did not come from Deliverables, assume that it's an asset
        logger.debug(s"File path $filePath came from rushes")
        val result = asLookup.relativizeFilePath(path)
        logger.debug(s"Relative path result is $result")
        result
      }
    })
  }

  private def addOrUpdateArchiveRecord(archivedRecord: Option[ArchivedRecord], filePath: String, uploadedPath: String, fileSize: Long,
                               itemId: Option[String], essenceVersion: Option[Int]):Future[Either[String, MessageProcessorReturnValue]] = {
    val record = archivedRecord match {
      case Some(rec) =>
        logger.debug(s"actual archivehunter ID for $filePath is ${rec.archiveHunterID}")
        rec.copy(
          originalFileSize = fileSize,
          uploadedPath = uploadedPath,
          vidispineItemId = itemId,
          vidispineVersionId = essenceVersion
        )
      case None =>
        val archiveHunterID = utils.ArchiveHunter.makeDocId(bucket = mediaFileUploader.bucketName, uploadedPath)
        logger.debug(s"Provisional archivehunter ID for $uploadedPath is $archiveHunterID")
        ArchivedRecord(None,
          archiveHunterID,
          archiveHunterIDValidated=false,
          originalFilePath=filePath,
          originalFileSize=fileSize,
          uploadedBucket = mediaFileUploader.bucketName,
          uploadedPath = uploadedPath,
          uploadedVersion = None,
          vidispineItemId = itemId,
          vidispineVersionId = essenceVersion,
          None,
          None,
          None,
          None,
          None
        )
    }

    logger.info(s"Updating record for ${record.originalFilePath} with vidispine ID ${itemId}, vidispine version ${essenceVersion}. ${if(!record.archiveHunterIDValidated) "ArchiveHunter ID validation is required."}")
    archivedRecordDAO
      .writeRecord(record)
      .map(recId=>Right(record.copy(id=Some(recId)).asJson))
  }

  private def uploadCreateOrUpdateRecord(filePath:String, relativePath:String, mediaIngested: VidispineMediaIngested,
                                         archivedRecord: Option[ArchivedRecord]):Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"Archiving file '$filePath' to s3://${mediaFileUploader.bucketName}/$relativePath")
    Future.fromTry(
      mediaFileUploader.copyFileToS3(new File(filePath), Some(relativePath))
    ).flatMap(fileInfo => {
      val (fileName, fileSize) = fileInfo
      logger.debug(s"$filePath: Upload completed")
      addOrUpdateArchiveRecord(archivedRecord, filePath, fileName, fileSize, mediaIngested.itemId, mediaIngested.essenceVersion)
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
                                   mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.debug(s"uploadIfRequiredAndNotExists: Original file is $filePath, target path is $relativePath")
    for {
      maybeArchivedRecord <- archivedRecordDAO.findBySourceFilename(filePath)
      maybeIgnoredRecord <- ignoredRecordDAO.findBySourceFilename(filePath)
      maybeFailureRecord <- failureRecordDAO.findBySourceFilename(filePath)
      result <- (maybeIgnoredRecord, maybeArchivedRecord) match {
        case (Some(ignoreRecord), _) =>
          Future.failed(SilentDropMessage(Some(s"${filePath} should be ignored due to reason ${ignoreRecord.ignoreReason}")))
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
                  .map(recId=>Right(MessageProcessorReturnValue(record.copy(id=Some(recId)).asJson)))
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
  def handleIngestedMedia(mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = {
    val status = mediaIngested.status
    val itemId = mediaIngested.itemId

    logger.debug(s"Received message content $mediaIngested")
    if (status.contains("FAILED") || itemId.isEmpty) {
      logger.error(s"Import status not in correct state for archive $status itemId=${itemId}")
      Future.failed(new RuntimeException(s"Import status not in correct state for archive $status itemId=${itemId}"))
    } else {
      mediaIngested.sourceOrDestFileId match {
        case Some(fileId)=>
          logger.debug(s"Got ingested file ID $fileId from the message")
          for {
            absPath <- vidispineCommunicator.getFileInformation(fileId).map(_.flatMap(_.getAbsolutePath))
            result <- absPath match {
              case None=>
                logger.error(s"Could not get absolute filepath for file $fileId")
                Future.failed(new RuntimeException(s"Could not get absolute filepath for file $fileId"))
              case Some(absPath)=>
                getRelativePath(absPath, mediaIngested.importSource) match {
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
    val uploadKey = uploadKeyForProxy(archivedRecord, fileInfo)

    fileInfo.uri.headOption.flatMap(u=>Try { URI.create(u)}.toOption) match {
      case Some(uri)=>
        val filePath = Paths.get(uri)
        if(internalCheckFile(filePath)) {
          logger.info(s"Starting upload of ${fileInfo.uri.headOption} to s3://${proxyFileUploader.bucketName}/$uploadKey")
          Future.fromTry(proxyFileUploader.copyFileToS3(filePath.toFile, Some(uploadKey)))
        } else {
          logger.error(s"Could not find path for URI $uri ($filePath) on-disk")
          Future.failed(new RuntimeException(s"File $filePath could not be found"))
        }
      case None=>
        logger.error(s"Either ${fileInfo.uri} is empty or it does not contain a valid URI")
        Future.failed(new RuntimeException(s"Fileinfo $fileInfo has no valid URI"))
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
                } yield Right(MessageProcessorReturnValue(updatedRecord.asJson))

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

  def handleShapeUpdate(shapeId:String, shapeTag:String, itemId:String): Future[Either[String, MessageProcessorReturnValue]] = {
    for {
      maybeArchivedItem <- archivedRecordDAO.findByVidispineId(itemId)
      maybeIgnoredItem <- ignoredRecordDAO.findByVidispineId(itemId)
      result <- (maybeArchivedItem, maybeIgnoredItem) match {
        case (_, Some(ignoredItem))=>
          logger.info(s"Item $itemId is ignored because ${ignoredItem.ignoreReason}, leaving alone")
          Future(Right(MessageProcessorReturnValue(ignoredItem.asJson)))
        case (Some(archivedItem), None)=>
          if(archivedItem.archiveHunterIDValidated) {
            for {
              proxyUploadResult <- uploadShapeIfRequired(itemId, shapeId, shapeTag, archivedItem)
              _ <- uploadThumbnailsIfRequired(itemId, None, archivedItem)
            } yield proxyUploadResult
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

  def uploadMetadataToS3(itemId: String, essenceVersion: Option[Int], archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] = {
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
        } yield Right(MessageProcessorReturnValue(updatedRecord.asJson))
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

  def handleMetadataUpdate(msg: Json, mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = {
    mediaIngested.itemId match {
      case Some(itemId) =>
        for {
          maybeArchivedItem <- archivedRecordDAO.findByVidispineId(itemId)
          maybeIgnoredItem <- ignoredRecordDAO.findByVidispineId(itemId)
          result <- (maybeArchivedItem, maybeIgnoredItem) match {
            case (_, Some(ignoredItem))=>
              logger.info(s"Item $itemId is ignored because ${ignoredItem.ignoreReason}, leaving alone")
              Future.failed(SilentDropMessage())
            case (Some(archivedItem), None)=>
              if(archivedItem.archiveHunterIDValidated) {
                uploadMetadataToS3(itemId, mediaIngested.essenceVersion, archivedItem)
              } else {
                logger.info(s"ArchiveHunter ID for ${archivedItem.originalFilePath} has not been validated yet")
                Future(Left(s"ArchiveHunter ID for ${archivedItem.originalFilePath} has not been validated yet"))
              }
            case (None, None)=>
              logger.info(s"No record of vidispine item $itemId")
              Future(Left(s"No record of vidispine item $itemId"))
          }
        } yield result
      case None =>
        logger.error("Metadata update without any item ID")
        Future.failed(new RuntimeException(s"Received metadata update ${msg.noSpaces} without any itemId"))
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
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"Received message from vidispine with routing key $routingKey")
    (msg.as[VidispineMediaIngested], routingKey) match {
      case (Left(err), _) =>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a VidispineMediaIngested: $err"))
      case (Right(mediaIngested), "vidispine.job.essence_version.stop")=>
        handleIngestedMedia(mediaIngested)
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
      case (Right(updatedMetadata), "vidispine.item.metadata.modify")=>
        handleMetadataUpdate(msg, updatedMetadata)
      case (_, _)=>
        logger.warn(s"Dropping message $routingKey from vidispine exchange as I don't know how to handle it. This should be fixed in" +
          s" the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
