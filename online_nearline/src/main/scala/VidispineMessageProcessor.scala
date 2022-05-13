import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.helpers.{Copier, MetadataHelper}
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.storagetier.auth.HMAC.logger
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.{AssetSweeperNewFile, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.utils.FilenameSplitter
import com.gu.multimedia.storagetier.vidispine.{QueryableItem, ShapeDocument, VSShapeFile, VidispineCommunicator}
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.{CustomMXSMetadata, MatrixStoreConfig}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import org.slf4j.MDC

import java.net.URI
import java.nio.file.{Path, Paths}
import java.time.{Instant, ZonedDateTime}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
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

  protected def checkFileExists(filePath:Path) = {
    if (!Files.exists(filePath)){
      logger.error(s"File $filePath doesn't exist")
      throw SilentDropMessage(Some(s"File $filePath doesn't exist"))
    } else if(!Files.isRegularFile(filePath)) {
      logger.error(s"File $filePath is not a regular file")
      throw SilentDropMessage(Some(s"File $filePath is not a regular file"))
    }
  }

  protected def newCorrelationId: String = UUID.randomUUID().toString

  /**
   * performs a search on the given vault looking for a matching file (i.e. one with the same file name AND size).
   * If one exists, it will create a NearlineRecord linking that file to the given name, save it to the database and return it.
   * Intended for use if no record exists already but a file may exist on the storage.
   * @param vault vault to check
   * @param filePath absolute path to the file on-disk
   * @param mediaIngested VidispineMediaIngested object representing the incoming message. This function will always
   *                      return None if the file size is not set here.
   * @param shouldSave if set to false, then don't write the record to the database before returning. Defaults to `true`.
   * @return a Future, containing the newly saved NearlineRecord if a record is found or None if not.
   */
  def checkForPreExistingFiles(vault:Vault, filePath:Path, mediaIngested: QueryableItem, shouldSave:Boolean=true) = {
    val fileSizeFut = (mediaIngested.fileSize, mediaIngested.itemId) match {
      case (Some(fileSize), _) => Future(Some(fileSize))
      case (None, Some(itemId))=>
        logger.info(s"No file size provided in incoming message, looking based on file ID")
        vidispineCommunicator.findItemFile(itemId, "original").map(_.map(_.size))
      case (None,None)=>
        logger.error("Could not check for pre-existing files as neither file size nor item ID was provided")
        Future(None)
    }

    fileSizeFut.flatMap({
      case Some(fileSize) =>
        fileCopier
          .findMatchingFilesOnNearline(vault, filePath, fileSize)
          .flatMap(matches => {
            if (matches.isEmpty) {
              logger.info(s"Found no pre-existing archived files for $filePath")
              Future(None)
            } else {
              logger.info(s"Found ${matches.length} archived files for $filePath: ${matches.map(_.pathOrFilename).mkString(",")}")
              val newRec = NearlineRecord(
                objectId = matches.head.oid,
                originalFilePath = filePath.toString,
                correlationId = newCorrelationId
              )
              if (shouldSave) {
                nearlineRecordDAO.writeRecord(newRec).map(newId => Some(newRec.copy(id = Some(newId))))
              } else {
                Future(Some(newRec))
              }
            }
          })
      case None=>
        Future(None)
    })
  }

  /**
   * Upload ingested file if not already exist.
   *
   * @param absPath       path to the file that has been ingested
   * @param mediaIngested  the media object ingested by Vidispine
   *
   * @return String explaining which action took place
   */
  def uploadIfRequiredAndNotExists(vault: Vault, absPath: String,
                                   mediaIngested: QueryableItem): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.debug(s"uploadIfRequiredAndNotExists: Original file is $absPath")

    val fullPath = Paths.get(absPath)
    checkFileExists(fullPath)

    val recordsFut = for {
      maybeNearlineRecord <- nearlineRecordDAO.findBySourceFilename(absPath)
      maybeUpdatedRecord <- if(maybeNearlineRecord.isEmpty) checkForPreExistingFiles(vault, fullPath, mediaIngested) else Future(maybeNearlineRecord)
      maybeFailureRecord <- failureRecordDAO.findBySourceFilename(absPath)
    } yield (maybeUpdatedRecord, maybeFailureRecord)

    recordsFut.flatMap(result => {
      val (maybeNearlineRecord, maybeFailureRecord) = result
      val maybeObjectId = maybeNearlineRecord.map(rec => rec.objectId)

      showPreviousFailure(maybeFailureRecord, absPath)

      fileCopier.copyFileToMatrixStore(vault, fullPath.getFileName.toString, fullPath)
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
                  None,
                  correlationId = newCorrelationId
                )
            }
            MDC.put("correlationId", record.correlationId)

            for {
              recId <- nearlineRecordDAO.writeRecord(record)
              updatedRecord <- Future(record.copy(id=Some(recId)))
              //ensure that Vidispine is updated with the MXS ID whenever the media changes
              result <- VidispineHelper.updateVidispineWithMXSId(mediaIngested.itemId.get, updatedRecord)
            } yield result

          case Left(error) => Future(Left(error))
        })
    }).recoverWith({
      case err:Throwable=>
        logger.error(s"Can't copy to MXS: ${err.getMessage}", err)

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
        Future(Left(s"No file exists on shape $shapeId for item $itemId yet"))
      case Some(fileInfo) =>
        val uriList = filePathsForShape(fileInfo)
        if(uriList.isEmpty) {
          logger.error(s"Either ${fileInfo.uri} is empty or it does not contain a valid URI")
          Future.failed(new RuntimeException(s"Fileinfo $fileInfo has no valid URI"))
        } else {
          val filePaths = uriList.map(Paths.get)
          firstCheckedFilepath(filePaths) match {
            case Some(filePath)=>
              Future(Right(filePath))
            case None=>
              logger.error(s"Could not find path for any of URI ${fileInfo.uri} on-disk")
              Future.failed(new RuntimeException(s"File any of URI ${fileInfo.uri} for Vidispine shape could not be found"))
            }
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
      copyResult <- fileCopier.copyFileToMatrixStore(vault, proxyFileName, fullPath)
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
          MDC.put("correlationId", nearlineRecord.correlationId)

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

  /**
   * queries Vidispine to find the paths of all files associated with the original shape of the given item ID.
   * Returns an empty sequence if there are none (e.g. if there is no original shape, no item, or no files on the original shape)
   * or a sequence of VSShapeFiles representing the files of the original shape if successful. Under normal circumstances
   * we would expect no more than one entry in the sequence
   * @param itemId item ID to query
   * @return list of matching files as `VSShapeFile` objects. Empty sequence otherwise.
   */
  def getOriginalFilesForItem(itemId:String) = {
    vidispineCommunicator
      .listItemShapes(itemId)
      .map(_.getOrElse(Seq()))
      .map(_.filter(_.tag.contains("original")))
      .map(shapes=>{
        shapes.map(_.getLikelyFile)
      })
      .map(_.collect({case Some(f)=>f}))
  }

  /**
   * sent if an old item does not have `gnm_nearline_id` set on it.
   * We need to check if we have a backup copy of the item and if so update, if not make one
   * @param vault
   * @param itemId
   */
  def handleVidispineItemNeedsBackup(vault:Vault, itemId:String) = {
    nearlineRecordDAO.findByVidispineId(itemId).flatMap({
      case Some(existingRecord) =>
        logger.info(s"Item $itemId is registered in the nearline database with MXS ID ${existingRecord.objectId}, updating Vidispine...")
        VidispineHelper.updateVidispineWithMXSId(itemId, existingRecord)
      case None =>
        val itemShapesFut = vidispineCommunicator.listItemShapes(itemId).map({
          case None =>
            logger.error(s"Can't back up vidispine item $itemId as it has no shapes on it")
            throw SilentDropMessage(Some("item has no shapes"))
          case Some(shapes) =>
            shapes.find(_.tag.contains("original")) match {
              case None =>
                logger.error(s"Can't back up vidispine item $itemId as it has no original shape. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
                throw SilentDropMessage(Some("item has no original shape"))
              case Some(originalShape) => originalShape
            }
        })

        val itemMetadataFut = vidispineCommunicator.getMetadata(itemId).map({
          case None =>
            logger.error(s"Can't get any metadata for item $itemId")
            Left("Can't get metadata")
          case Some(meta) =>
            val possibleNearlineIds = meta.valuesForField("gnm_nearline_id", Some("Asset"))
            if (possibleNearlineIds.isEmpty || possibleNearlineIds.count(_.value.nonEmpty) == 0) {
              logger.info(s"Item $itemId does not appear to have a registered nearline copy, attempting to correct")
              Right(meta)
            } else {
              logger.info(s"Item $itemId does have a registered nearline ID: ${possibleNearlineIds.filter(_.value.nonEmpty).map(_.value).mkString("; ")}, not going to do anything")
              throw SilentDropMessage(Some("item already has a registered nearline copy"))
            }
        })

        for {
          metadata <- itemMetadataFut
          itemShapes <- itemShapesFut
          maybePath <- itemShapes.getLikelyFile.map(_.id) match {
            case Some(fileId)=>
              vidispineCommunicator.getFileInformation(fileId).map(_.flatMap(_.getAbsolutePath))
            case None=>
              Future(None)
          }
          result <- (metadata, maybePath) match {
            case (Right(meta), Some(absPath))=>
              logger.info(s"$itemId: Checking if there is a matching file in the nearline and uploading if necessary...")
              uploadIfRequiredAndNotExists(vault, absPath, QueryableVidispineItemResponse(meta, itemShapes))
            case (_, None)=>
              Future(Left(s"Could not determine a file path for $itemId"))
            case (Left(err), _)=>
              Future(Left(err))
          }
        } yield result
    })
  }

  def handleMetadataUpdate(metadataUpdated:VidispineMediaIngested):Future[Either[String, MessageProcessorReturnValue]] = {
    metadataUpdated.itemId match {
      case None=>
        logger.error(s"Incoming vidispine message $metadataUpdated has no itemId")
        Future.failed(new RuntimeException("no vidispine item id provided"))
      case Some(itemId)=>
        matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault=>
          nearlineRecordDAO
            .findByVidispineId(itemId)
            .flatMap({
              case foundRecord@Some(_)=>Future(foundRecord) //we found a record in the database, happy days
              case None=>                                   //we didn't find a record in the database, but maybe the file does exist already?
                val maybeNewEntryList = for {
                  files <- getOriginalFilesForItem(itemId)
                  filePaths <- Future(files.map(_.path).map(Paths.get(_)))
                  possibleEntries <- {
                    logger.info(s"Checking for existing files for vidispine ID $itemId. File path list is $filePaths.")
                    Future.sequence(filePaths.map(path=>checkForPreExistingFiles(vault, path, metadataUpdated, shouldSave = false)))
                  }
                } yield possibleEntries

                maybeNewEntryList
                  .map(_.collect({case Some(newRec)=>newRec}))
                  .map(_.headOption)
                  .flatMap({
                    case Some(newRec)=>
                      //we got a record. Write it to the database and continue.
                      logger.info(s"Found existing file at ${newRec.originalFilePath} with OID ${newRec.objectId} for vidispine item ${newRec.vidispineItemId}")
                      nearlineRecordDAO
                        .writeRecord(newRec)
                        .map(recordId=>newRec.copy(id=Some(recordId)))
                        .map(Some(_))
                    case None=>
                      logger.info(s"No files found in the nearline storage for vidispine item ID $itemId")
                      Future(None)
                  })
                .recover({
                  case err:Throwable=>
                    logger.error(s"Could not search for pre-existing files for vidispine item $itemId: ${err.getMessage}", err)
                    None  //this will cause a retryable failure to be logged out below.
                })
            })
            .flatMap({
              case None=>
                logger.info(s"No record of vidispine item $itemId yet.")
                Future(Left(s"No record of vidispine item $itemId yet.")) //this is retryable, assume that the item has not finished importing yet
              case Some(nearlineRecord: NearlineRecord)=>
                  MDC.put("correlationId", nearlineRecord.correlationId)
                  buildMetaForXML(vault, nearlineRecord, itemId).flatMap({
                    case None=>
                      //this is a retryable failure; sometimes the "updated metadata" message will arrive earlier than the media has finished processing.
                      logger.info(s"The object ${nearlineRecord.objectId} for file ${nearlineRecord.originalFilePath} does not have GNM compatible metadata attached to it yet. This can mean that ingest or extraction is still in progress.")
                      Future(Left(s"Object ${nearlineRecord.objectId} does not have GNM compatible metadata"))
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
              })
        }
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
      case (Right(mediaIngested), "vidispine.job.essence_version.stop")=>
        matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
          handleIngestedMedia(vault, mediaIngested)
        }
      case (Right(mediaIngested), "vidispine.item.metadata.modify")=>
        handleMetadataUpdate(mediaIngested)
      case (Right(mediaItemIdOnly), "vidispine.itemneedsbackup")=>
        matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
          handleVidispineItemNeedsBackup(vault, mediaItemIdOnly.itemId.get)
        }
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
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }
  }
}
