import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.helpers.{Copier, MetadataHelper}
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.storagetier.auth.HMAC.logger
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.VidispineMediaIngested
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.utils.FilenameSplitter
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VSShapeFile, VidispineCommunicator}
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.{CustomMXSMetadata, MatrixStoreConfig}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._

import java.net.URI
import java.nio.file.{Path, Paths}
import java.time.{Instant, ZonedDateTime}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.nio.file.{Files, Path, Paths}
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
   * check if a file exists. It's put into its own method so it can be over-ridden in tests
   * @param filePath path to check
   * @return boolean indicating if it exists
   */
  protected def internalCheckFile(filePath:Path) = Files.exists(filePath)

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
                uploadIfRequiredAndNotExists(vault, absPath, mediaIngested)
            }
          } yield result
        case None=>
          logger.error(s"The incoming message had no source file ID parameter, can't continue")
          Future.failed(new RuntimeException(s"No source file ID parameter"))
      }
    }
  }

  def getFilePathForShape(shapeDoc: ShapeDocument, itemId: String, shapeId: String): Future[Either[String,Path]] = {
    shapeDoc.getLikelyFile match {
      case None =>
        Future(Left(s"No file exists on shape $shapeId for item $itemId yet"))
      case Some(fileInfo) =>
        fileInfo.uri.headOption.flatMap(u => Try {
          URI.create(u)
        }.toOption) match {
          case Some(uri) =>
            val filePath = Paths.get(uri)
            if (internalCheckFile(filePath)) {
              logger.info(s"Found filePath for Vidispine shape ${filePath}")
              Future(Right(filePath))
            } else {
              logger.error(s"Could not find path for URI $uri ($filePath) on-disk")
              Future.failed(new RuntimeException(s"File $filePath for Vidispine shape could not be found"))
            }
          case None =>
            logger.error(s"Either ${fileInfo.uri} is empty or it does not contain a valid URI")
            Future.failed(new RuntimeException(s"Fileinfo $fileInfo has no valid URI"))
        }
    }
  }

  /**
   * Determines an appropriate file name to use for the proxy of the given file
   * @param nearlineRecord NearlineRecord representing the "original" media for this content
   * @param proxyFile VSShapeFile object representing the File portion that Vidispine returned
   * @return a String containing the path to upload to
   */
  def uploadKeyForProxy(nearlineRecord: NearlineRecord, proxyFile:VSShapeFile) = {
    val uploadedPath = Paths.get(nearlineRecord.originalFilePath)

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

  /**
   * calls MetadataHelper.setAttributeMetadata. Included like this to make test mocking easier
   * @param obj MxsObject on which to set metadata
   * @param meta MxsMetadata object containing the metadata to set
   */
  protected def callUpdateMetadata(vault:Vault, objectId:String, meta:MxsMetadata) = Try {
    val obj = vault.getObject(objectId)
    MetadataHelper.setAttributeMetadata(obj, meta)
  }

  /**
   * updates the given value on the objectmatrix file referenced.
   * Intended to be used to update the attachment (ATT_PROXY_OID, ATT_THUMB_OID) fields on original media
   * @param vault vault object representing the vault holding the file
   * @param objectId object ID of the file
   * @param fieldName field name to set
   * @param fieldValue field value to set
   * @return a Try with no value if successful, or a failure on exception
   */
  protected def updateParentsMetadata(vault:Vault, objectId:String, fieldName:String, fieldValue:String) = Try {
    val obj = vault.getObject(objectId)
    val view = obj.getAttributeView
    view.writeString(fieldName, fieldValue)
  }
  /**
   * Upload ingested shape if not already exist.
   *
   * @param filePath       path to the file that has been ingested
   * @param mediaIngested  the media object ingested by Vidispine
   * @param nearlineRecord the record of the existing files copied to MatrixStore
   * @param proxyFile      VSShapeFile representing the proxy file
   *
   * @return
   */
  def uploadShapeIfRequired(vault: Vault, fullPath: Path,
                            mediaIngested: VidispineMediaIngested,
                            nearlineRecord: NearlineRecord,
                            proxyFile:VSShapeFile): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.debug(s"uploadShapeIfRequired: Shape file is $fullPath")

    val proxyFileName = uploadKeyForProxy(nearlineRecord, proxyFile)

    val copyResult = for {
      copyResult <- fileCopier.copyFileToMatrixStore(vault, proxyFileName, fullPath, None)
      metadataUpdate <- buildMetadataForProxy(vault, nearlineRecord)
      writeResult <- Future.fromTry((copyResult, metadataUpdate) match {
        case (Left(_), _)=>Success( () ) //ignore
        case (Right(proxyObjectId), Some(meta))=>
          callUpdateMetadata(vault, proxyObjectId, meta)
        case (Right(proxyObjectId), None)=>
          logger.error(s"Could not get parent metadata for ${nearlineRecord.objectId} (${nearlineRecord.originalFilePath}). Deleting the copied proxy until we have the right metadata.")
          Try {
            val obj = vault.getObject(proxyObjectId)
            obj.delete()
          }
      })
      _ <- Future.fromTry(copyResult match {
        case Left(_)=>Success( () )
        case Right(proxyOid)=>updateParentsMetadata(vault, nearlineRecord.objectId, "ATT_PROXY_OID", proxyOid)
      })
    } yield copyResult

    copyResult.flatMap({
        case Right(objectId) =>
          val record = nearlineRecord
            .copy(
              proxyObjectId = Some(objectId),
              vidispineItemId = mediaIngested.itemId,
              vidispineVersionId = mediaIngested.essenceVersion,
            )

          nearlineRecordDAO
            .writeRecord(record)
            .map(recId=>
              Right(MessageProcessorReturnValue(
                record
                  .copy(id = Some(recId))
                  .asJson
              ))
            )

        case Left(error) => Future(Left(error))
      })
  }

  def copyShapeIfRequired(vault: Vault, mediaIngested: VidispineMediaIngested,
                          itemId: String, shapeId: String,
                          nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] = {
      vidispineCommunicator.findItemShape(itemId, shapeId).flatMap({
        case None=>
          logger.error(s"Shape $shapeId does not exist on item $itemId despite a notification informing us that it does.")
          Future.failed(new RuntimeException(s"Shape $shapeId does not exist"))
        case Some(shapeDoc)=>
          shapeDoc.getLikelyFile match {
            case None =>
              Future(Left(s"No file exists on shape $shapeId for item $itemId yet"))
            case Some(fileInfo) =>
              getFilePathForShape(shapeDoc, itemId, shapeId).flatMap({
                case Left(err) =>
                  logger.error(s"Can't find filePath for Shape with id $shapeId - err ${err}")
                  Future.failed(new RuntimeException(s"Shape $shapeId does not exist"))
                case Right(filePath) =>
                  val copyFut = for {
                    copyResult <- uploadShapeIfRequired(vault, filePath, mediaIngested, nearlineRecord, fileInfo)
                  } yield copyResult

                  //the future will fail if we can't copy to MatrixStore, but treat this as a retryable failure
                  copyFut.recover({
                    case err: Throwable =>
                      logger.error(s"Could not copy ${filePath.toString} to MatrixStore: ${err.getMessage}", err)
                      Left(s"Could not copy ${filePath.toString} to MatrixStore")
                  })
              })
          }
      })
  }

  def handleShapeUpdate(vault: Vault, mediaIngested: VidispineMediaIngested, shapeId:String, itemId:String)
  : Future[Either[String, MessageProcessorReturnValue]] = {
    nearlineRecordDAO.findByVidispineId(itemId).flatMap({
        case Some(nearlineRecord) =>
          copyShapeIfRequired(vault, mediaIngested, itemId, shapeId, nearlineRecord)
        case None =>
          logger.info(s"No record of vidispine item $itemId retry later")
          Future(Left(s"No record of vidispine item $itemId retry later"))
    })
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
   * build GNM metadata for a proxy file.
   * This is done by copying the GNM metadata for the original media and then modifiying the type and attachment fields
   * @param vault vault containing the files
   * @param rec NearlineRecord representing the item being processed
   * @return a Future, containing either
   */
  protected def buildMetadataForProxy(vault:Vault, rec:NearlineRecord) = {
    getOriginalMediaMeta(vault, rec.objectId)
      .map(_.map(_.copy(
        itemType = CustomMXSMetadata.TYPE_PROXY,
        vidispineItemId = rec.vidispineItemId,
        proxyOID = None,
        thumbnailOID = None,
        metaOID = None,
        mainMediaOID = Some(rec.objectId))))
      .map(_.map(_.toAttributes(MxsMetadata.empty)))  //filesystem level metadata should have been added at this point
  }

  /**
   * Builds a CustomMXSMetadata object for the metadata of the file indicated in the given NearlineRecord.
   * This calls `getOriginalMediaMeta` to look up the original file's metadata and then switches the `itemType`
   * field to TYPE_META
   * @param vault Vault object where the media lives
   * @param rec NearlineRecord indicating the file being worked with
   * @return a Future, cotnaining either the updated CustomMXSMetadata or None
   */
  protected def buildMetaForXML(vault:Vault, rec:NearlineRecord, itemId:String, nowTime:ZonedDateTime=ZonedDateTime.now()) = {
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
          "MXFS_FILEEXT"->".xml",
          "ATT_ORIGINAL_OID"->rec.objectId
        ),
        boolValues = Map(
          "MXFS_INTRASH"->false,
        ),
        longValues = Map(
          "MXFS_MODIFICATION_TIME"->nowTime.toInstant.toEpochMilli,
          "MXFS_CREATION_TIME"->nowTime.toInstant.toEpochMilli,
          "MXFS_ACCESS_TIME"->nowTime.toInstant.toEpochMilli,
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

  /**
   * helper method to call Copier.doStreamCopy. Included like this to make test mocking easier.
   */
  protected def callStreamCopy(source:Source[ByteString, Any], vault:Vault, updatedMetadata:MxsMetadata) =
    Copier.doStreamCopy(
      source,
      vault,
      updatedMetadata.toAttributes.toArray
    )

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

          callStreamCopy(httpEntity.dataBytes, vault, updatedMetadata)
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
                      updateParentsMetadata(vault, nearlineRecord.objectId, "ATT_META_OID", copiedId) match {
                        case Success(_) =>
                        case Failure(err)=>
                          //this is not a fatal error.
                          logger.warn(s"Could not update metadata on ${nearlineRecord.objectId} to set metadata attachment: ${err.getMessage}")
                      }

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
          case (_, Some("original"), _)=>
            logger.info(s"Shape tag original handled by new file event, dropping message")
            Future.failed(SilentDropMessage())
          case (Some(shapeId), Some(_), Some(itemId))=>
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
              handleShapeUpdate(vault, shapeUpdate, shapeId, itemId)
            }
        }
      case (_, _)=>
        logger.warn(s"Dropping message $routingKey from vidispine exchange as I don't know how to handle it. This should be fixed in" +
          s" the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
