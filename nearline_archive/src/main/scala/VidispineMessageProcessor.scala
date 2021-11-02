import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper.{categoryForMimetype, getFileExt}
import com.gu.multimedia.mxscopy.{MXSConnectionBuilder, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.helpers.{Copier, MetadataHelper}
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.storagetier.auth.HMAC.logger
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue}
import com.gu.multimedia.storagetier.messages.VidispineMediaIngested
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.{CustomMXSMetadata, MatrixStoreConfig}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._

import java.nio.file.{Path, Paths}
import java.nio.file.attribute.FileTime
import java.time.{Instant, ZonedDateTime}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

class VidispineMessageProcessor()
                               (implicit nearlineRecordDAO: NearlineRecordDAO,
                                failureRecordDAO: FailureRecordDAO,
                                vidispineCommunicator: VidispineCommunicator,
                                ec: ExecutionContext,
                                mat: Materializer,
                                system: ActorSystem,
                                matrixStoreBuilder: MXSConnectionBuilder,
                                mxsConfig: MatrixStoreConfig,
                                fileCopier: FileCopier) extends MessageProcessor {

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
  def uploadIfRequiredAndNotExists(vault: Vault, absPath: String,
                                   mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.debug(s"uploadIfRequiredAndNotExists: Original file is $absPath")

    val fullPath = Paths.get(absPath)
    val fullPathFile = fullPath.toFile

    if (!fullPathFile.exists || !fullPathFile.isFile) {
      logger.info(s"File ${absPath} doesn't exist")
      Failure(new Exception(s"File ${absPath} doesn't exist"))
    }

    val recordsFut = for {
      maybeNearlineRecord <- nearlineRecordDAO.findBySourceFilename(absPath)
      maybeFailureRecord <- failureRecordDAO.findBySourceFilename(absPath)
    } yield (maybeNearlineRecord, maybeFailureRecord)

    recordsFut.flatMap(result => {
      val (maybeNearlineRecord, maybeFailureRecord) = result
      val maybeObjectId = maybeNearlineRecord.map(rec => rec.objectId)

      showPreviousFailure(maybeFailureRecord, absPath)

      fileCopier.copyFileToMatrixStore(vault, fullPath.getFileName.toString, fullPath, maybeObjectId)
        .flatMap({
          case Right(objectId) =>
            val record = maybeNearlineRecord match {
              case Some(rec) =>
                rec
                  .copy(
                    objectId = objectId,
                    originalFilePath = fullPath.toString,
                    vidispineItemId = mediaIngested.itemId,
                    vidispineVersionId = mediaIngested.essenceVersion
                  )
              case None =>
                NearlineRecord(
                  None,
                  objectId = objectId,
                  originalFilePath = fullPath.toString,
                  vidispineItemId = mediaIngested.itemId,
                  vidispineVersionId = mediaIngested.essenceVersion,
                  None,
                  None
                )
            }

            nearlineRecordDAO
              .writeRecord(record)
              .map(recId=>
                //for some reason using the implicit conversion causes a compilation error here - https://github.com/scala/bug/issues/6317
                Right(MessageProcessorReturnValue(record.copy(id=Some(recId)).asJson))
              )

          case Left(error) => Future(Left(error))
        })
    }).recoverWith(err=>{
      val attemptCount = attemptCountFromMDC() match {
        case Some(count)=>count
        case None=>
          logger.warn(s"Could not get attempt count from logging context for $fullPath, creating failure report with attempt 1")
          1
      }

      val rec = FailureRecord(id = None,
        originalFilePath = fullPath.toString,
        attempt = attemptCount,
        errorMessage = err.getMessage,
        errorComponent = ErrorComponents.Internal,
        retryState = RetryStates.WillRetry)
      failureRecordDAO.writeRecord(rec).map(_=>Left(err.getMessage))
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
  def handleIngestedMedia(vault: Vault, mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = {
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
                uploadIfRequiredAndNotExists(vault: Vault, absPath, mediaIngested)
            }
          } yield result
        case None=>
          logger.error(s"The incoming message had no source file ID parameter, can't continue")
          Future.failed(new RuntimeException(s"No source file ID parameter"))
      }
    }
  }

  /**
   * Gets the metadata for the given object ID on the given vault, and tries to convert it into a CustomMXSMetadata object.
   * If the metadata is not compatible, None is returned otherwise the CustomMXSMetadata is returned in an Option.
   * @param vault Vault object with which to do the lookup
   * @param mediaOID object ID of the media on the vault
   * @return a Future, containing either the CustomMXSMetadata or None
   */
  protected def getOriginalMediaMeta(vault:Vault, mediaOID:String) = {
    for {
      omFile <- Future.fromTry(Try { vault.getObject(mediaOID)})
      meta <- MetadataHelper.getAttributeMetadata(omFile)
    } yield CustomMXSMetadata.fromMxsMetadata(meta)
  }

  /**
   * Builds a CustomMXSMetadata object for the metadata of the file indicated in the given NearlineRecord.
   * This calls `getOriginalMediaMeta` to look up the original file's metadata and then switches the `itemType`
   * field to TYPE_META
   * @param vault Vault object where the media lives
   * @param rec NearlineRecord indicating the file being worked with
   * @return a Future, cotnaining either the updated CustomMXSMetadata or None
   */
  protected def buildMetaForXML(vault:Vault, rec:NearlineRecord, itemId:String) = {
    val nowTime = ZonedDateTime.now()
    def makeBaseMeta(basePath:Path) = {
      val filePath = basePath.resolve(s"$itemId.XML")
      MxsMetadata(
        stringValues = Map(
          "MXFS_FILENAME_UPPER" -> filePath.toString.toUpperCase,
          "MXFS_FILENAME"-> s"$itemId.xml",
          "MXFS_PATH"-> filePath.toString,
          "MXFS_MIMETYPE"-> "application/xml",
          "MXFS_DESCRIPTION"->s"Vidispine metadata for $itemId",
          "MXFS_PARENTOID"->"",
          "MXFS_FILEEXT"->".xml"
        ),
        boolValues = Map(
          "MXFS_INTRASH"->false,
        ),
        longValues = Map(
          "MXFS_MODIFICATION_TIME"->Instant.now().toEpochMilli,
          "MXFS_CREATION_TIME"->Instant.now().toEpochMilli,
          "MXFS_ACCESS_TIME"->Instant.now().toEpochMilli,
        ),
        intValues = Map(
          "MXFS_CREATIONDAY"->nowTime.getDayOfMonth,
          "MXFS_COMPATIBLE"->1,
          "MXFS_CREATIONMONTH"->nowTime.getMonthValue,
          "MXFS_CREATIONYEAR"->nowTime.getYear,
          "MXFS_CATEGORY"->4, //"document"
        )
      )
    }

    getOriginalMediaMeta(vault, rec.objectId).map(maybeMeta=>{
      for {
        updatedMeta <- maybeMeta.map(_.copy(itemType = CustomMXSMetadata.TYPE_META))
        baseMeta <- Some(makeBaseMeta(Paths.get(rec.originalFilePath).getParent))
        mxsMeta <- Some(updatedMeta.toAttributes(baseMeta))
      } yield mxsMeta
    })
  }

  protected def streamVidispineMeta(vault:Vault, itemId:String, objectMetadata:MxsMetadata) =
    vidispineCommunicator
      .akkaStreamXMLMetadataDocument(itemId)
      .flatMap({
        case Some(httpEntity)=>
          val updatedMetadata = httpEntity.contentLengthOption match {
            case Some(length)=>objectMetadata.withValue("DPSP_SIZE",length)
            case None=>
              logger.warn(s"Metadata response for $itemId has no content-length header, DPSP_SIZE will not be set")
              objectMetadata
          }

          Copier.doStreamCopy(httpEntity.dataBytes,
            vault,
            updatedMetadata.toAttributes.toArray
          )
            .map(Right.apply)
            .recover({
              case err:Throwable=>
                logger.error(s"Could not copy metadata for item $itemId to vault ${vault.getId}: ${err.getMessage}", err)
                Left(s"Could not copy metadata for item $itemId")
            })
        case None=>
          logger.error(s"Vidispine item $itemId does not appear to have any metadata")
          Future.failed(new RuntimeException(s"No metadata on $itemId"))  //this is a permanent failure, no point in retrying
      })

  def handleMetadataUpdate(metadataUpdated:VidispineMediaIngested):Future[Either[String, MessageProcessorReturnValue]] = {
    metadataUpdated.itemId match {
      case None=>
        logger.error(s"Incoming vidispine message $metadataUpdated has no itemId")
        Future.failed(new RuntimeException("no vidispine item id provided"))
      case Some(itemId)=>
        nearlineRecordDAO.findByVidispineId(itemId).flatMap({
          case None=>
            logger.info(s"No record of vidispine item $itemId yet.")
            Future(Left(s"No record of vidispine item $itemId yet.")) //this is retryable, assume that the item has not finished importing yet
          case Some(nearlineRecord: NearlineRecord)=>
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault=>
              buildMetaForXML(vault, nearlineRecord, itemId).flatMap({
                case None=>
                  logger.error(s"The object ${nearlineRecord.objectId} for file ${nearlineRecord.originalFilePath} does not have GNM compatible metadata attached to it")
                  Future.failed(new RuntimeException(s"Object ${nearlineRecord.objectId} does not have GNM compatible metadata")) //this is a permanent failure
                case Some(updatedMetadata)=>
                  streamVidispineMeta(vault, itemId, updatedMetadata).flatMap({
                    case Right((copiedId, maybeChecksum))=>
                      logger.info(s"Metadata xml for $itemId is copied to file $copiedId with checksum ${maybeChecksum.getOrElse("(none)")}")
                      val updatedRec = nearlineRecord.copy(metadataXMLObjectId = Some(copiedId))
                      nearlineRecordDAO
                        .writeRecord(updatedRec)
                        .map(_=>Right(updatedRec.asJson))
                    case Left(err)=>Future(Left(err))
                  })

              })
            }
        })
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
      case (Right(mediaIngested), "vidispine.job.raw_import.stop")=>
        matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
          handleIngestedMedia(vault, mediaIngested)
        }
      case (Right(mediaIngested), "vidispine.item.metadata.modify")=>
        handleMetadataUpdate(mediaIngested)
      case (_, _)=>
        logger.warn(s"Dropping message $routingKey from vidispine exchange as I don't know how to handle it. This should be fixed in" +
          s" the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
