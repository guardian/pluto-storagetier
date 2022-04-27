import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.ArchiveHunterCommunicator
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.VidispineMediaIngested
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecord, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.utils.FilenameSplitter
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VSShapeFile, VidispineCommunicator}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.slf4j.LoggerFactory
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig}
import plutodeliverables.PlutoDeliverablesConfig
import utils.ArchiveHunter
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._

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
}

class VidispineMessageProcessor(plutoCoreConfig: PlutoCoreConfig, deliverablesConfig: PlutoDeliverablesConfig)
                               (implicit archivedRecordDAO: ArchivedRecordDAO,
                                failureRecordDAO: FailureRecordDAO,
                                ignoredRecordDAO: IgnoredRecordDAO,
                                vidispineCommunicator: VidispineCommunicator,
                                archiveHunterCommunicator: ArchiveHunterCommunicator,
                                vidispineFunctions: VidispineFunctions,
                                ec: ExecutionContext,
                                mat: Materializer,
                                system: ActorSystem) extends MessageProcessor {
  protected lazy val asLookup = new AssetFolderLookup(plutoCoreConfig)
  private val logger = LoggerFactory.getLogger(getClass)
  import VidispineFunctions._
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
        val archiveHunterID = utils.ArchiveHunter.makeDocId(bucket = vidispineFunctions.mediaBucketName, uploadedPath)
        logger.debug(s"Provisional archivehunter ID for $uploadedPath is $archiveHunterID")
        ArchivedRecord(None,
          archiveHunterID,
          archiveHunterIDValidated=false,
          originalFilePath=filePath,
          originalFileSize=fileSize,
          uploadedBucket = vidispineFunctions.mediaBucketName,
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

  /**
   * Creates an "ignore" record in the database so that subsequent VS events on this item are dropped and don't loop
   * @param fullPath absolute path of the file on SAN
   * @param vidispineItemId vidispine item ID. This is taken as an option because it comes through from the notification as
   *                        an option, but is not expected to be null.
   * @param vidispineVersion vidispine version ID as passed from the notification
   * @param reason string reason why the item was ignored.
   * @return a Future containing the saved IgnoreRecord
   */
  private def setIgnoredRecord(fullPath:String, vidispineItemId:Option[String], vidispineVersion:Option[Int], reason:String) = {
    val rec = IgnoredRecord(None, fullPath, reason, vidispineItemId, vidispineVersion)
    //record the fact we ignored the file to the database. This should not raise duplicate record errors.
    ignoredRecordDAO
      .writeRecord(rec)
      .map(recId=>rec.copy(id=Some(recId)))
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
            maybeProject <- absPath.map(Paths.get(_)).map(asLookup.assetFolderProjectLookup).getOrElse(Future(None))
            result <- (absPath, maybeProject) match {
              case (None, _)=>
                logger.error(s"Could not get absolute filepath for file $fileId")
                Future.failed(new RuntimeException(s"Could not get absolute filepath for file $fileId"))
              case (Some(absPath), Some(projectInfo)) =>
                if(projectInfo.deletable.contains(true) || projectInfo.sensitive.contains(true)) {
                  setIgnoredRecord(absPath, mediaIngested.itemId, mediaIngested.essenceVersion, "Project was either deletable or sensitive")
                    .flatMap(_=>
                      Future.failed(SilentDropMessage(Some(s"$absPath is from project ${projectInfo.id} which is either deletable or sensitive")))
                    )
                } else {
                  getRelativePath(absPath, mediaIngested.importSource) match {
                    case Left(err) =>
                      logger.error(s"Could not relativize file path $absPath: $err. Uploading to $absPath")
                      vidispineFunctions.uploadIfRequiredAndNotExists(absPath, absPath, mediaIngested)
                    case Right(relativePath) =>
                      vidispineFunctions.uploadIfRequiredAndNotExists(absPath, relativePath.toString, mediaIngested)
                  }
                }
              case (_, None) =>
                Future(Left(s"Could not look up a project for fileId $fileId ($absPath)"))
            }
          } yield result.map(MessageProcessorReturnValue.apply)
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
  def handleRawImportStop(msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    msg.as[VidispineMediaIngested] match {
      case Left(err) =>
        logger.error(s"Could not unmarshal vidispine.job.raw_import.stop message: $err")
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a VidispineMediaIngested: $err"))
      case Right(mediaIngested) =>
        handleIngestedMedia(mediaIngested)
    }
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
              proxyUploadResult <- vidispineFunctions.uploadShapeIfRequired(itemId, shapeId, shapeTag, archivedItem)
              _ <- vidispineFunctions.uploadThumbnailsIfRequired(itemId, None, archivedItem)
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

  def handleMetadataUpdate(msg: Json, mediaIngested: VidispineMediaIngested): Future[Either[String, Json]] = {
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
                vidispineFunctions.uploadMetadataToS3(itemId, mediaIngested.essenceVersion, archivedItem)
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
