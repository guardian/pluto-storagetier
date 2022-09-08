import MediaNotRequiredMessageProcessor.Action
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.helpers.{MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class MediaNotRequiredMessageProcessor(asLookup: AssetFolderLookup)(
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
  vidispineCommunicator: VidispineCommunicator,
  s3ObjectChecker: S3ObjectChecker,
  checksumChecker: ChecksumChecker
) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def newCorrelationId: String = UUID.randomUUID().toString

  def getOnlineSize(pendingDeletionRecord: PendingDeletionRecord, incomingItemId: Option[String]): Future[Option[Long]] =
    ((pendingDeletionRecord.vidispineItemId, incomingItemId) match {
      case (Some(itemIdFromPendingDeletionRecord), _) => getVidispineSize(itemIdFromPendingDeletionRecord)
      case (_, Some(itemIdFromIncoming)) => getVidispineSize(itemIdFromIncoming)
      case (_, _) =>
        logger.warn(s"Cannot get size for ${pendingDeletionRecord.originalFilePath} from online without an item ID")
        throw new RuntimeException(s"Cannot get size for ${pendingDeletionRecord.originalFilePath} from online without an item ID")
    }).map({
      case Left(err) => throw new RuntimeException(s"no size, because: $err")
      case Right(sizeMaybe) => sizeMaybe
    })


  def getVidispineSize(itemId: String): Future[Either[String, Option[Long]]] =
    vidispineCommunicator.listItemShapes(itemId).map({
      case None =>
        logger.error(s"Can't get size of vidispine item $itemId as it has no shapes on it")
        Left(s"Can't get size of vidispine item $itemId as it has no shapes on it")
      case Some(shapes) =>
        shapes.find(_.tag.contains("original")) match {
          case None =>
            logger.error(s"Can't get size of vidispine item $itemId as it has no original shape. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
            Left(s"Can't get size of vidispine item $itemId as it has no original shape. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
          case Some(originalShape) =>
            Right(originalShape.getLikelyFile.flatMap(_.sizeOption))
        }
    })


  def handleInternalArchiveCompleteForNearline(vault: Vault, internalArchiveVault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(nearlineRecord.originalFilePath, MediaTiers.NEARLINE).flatMap({ // FIXME this is not a unique identifer - several nearline files can have the same path e.g.
      case Some(pendingDeletionRecord) =>
        pendingDeletionRecord.nearlineId match {
          case Some(nearlineId) =>
            val nearlineFileSize = Future.fromTry(Try { vault.getObject(nearlineId) }.map(MetadataHelper.getFileSize)) // We fetch the current size, because we don't know how old the message is
            nearlineFileSize.flatMap(fileSize => {
              nearlineExistsInInternalArchive(vault, internalArchiveVault, nearlineId, pendingDeletionRecord.originalFilePath, fileSize)
                .flatMap({
                  case true =>
                    for {
                      mediaRemovedMsg <- deleteMediaFromNearline(vault, pendingDeletionRecord.mediaTier.toString, Some(pendingDeletionRecord.originalFilePath), pendingDeletionRecord.nearlineId, pendingDeletionRecord.vidispineItemId)
                      _ <- removeDeletionPending(pendingDeletionRecord)
                    } yield mediaRemovedMsg
                  case false =>
                    pendingDeletionRecordDAO.updateAttemptCount(pendingDeletionRecord.id.get, pendingDeletionRecord.attempt + 1)
                    NOT_IMPL_outputInternalArchiveCopyRequired(pendingDeletionRecord)
                })
            })
          case None => throw new RuntimeException("NEARLINE pending deletion record w/o nearline id!")
        }
      case None =>
        throw SilentDropMessage(Some(s"ignoring internal archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${nearlineRecord.originalFilePath}"))
    })
   }


  def handleInternalArchiveCompleteForOnline(internalArchiveVault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(nearlineRecord.originalFilePath, MediaTiers.NEARLINE).flatMap({
      case Some(pendingDeletionRecord) =>
        pendingDeletionRecord.vidispineItemId match {
          case Some(vsItemId) =>
              getOnlineSize(pendingDeletionRecord, nearlineRecord.vidispineItemId).flatMap({
                case Some(onlineSize) =>
                  onlineExistsInVault(internalArchiveVault, vsItemId, pendingDeletionRecord.originalFilePath, onlineSize).flatMap({
                    case true =>
                      for {
                        mediaRemovedMessage <- deleteMediaFromOnline(pendingDeletionRecord)
                        _ <- pendingDeletionRecordDAO.deleteRecord(pendingDeletionRecord)
                      } yield mediaRemovedMessage
                    case false =>
                      pendingDeletionRecordDAO.updateAttemptCount(pendingDeletionRecord.id.get, pendingDeletionRecord.attempt + 1)
                      NOT_IMPL_outputInternalArchiveCopyRequired(pendingDeletionRecord)
                  })
                case None =>
                  logger.info(s"Could not get online size from Vidispine, retrying")
                  Future(Left(s"Could not get online size from Vidispine, retrying"))
              })
          case None => throw new RuntimeException("NEARLINE pending deletion record w/o nearline id!")
        }
      case None =>
        throw SilentDropMessage(Some(s"ignoring internal archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${nearlineRecord.originalFilePath}"))
    })
   }


  def handleDeepArchiveCompleteOrReplayedForOnline(archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
    pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(archivedRecord.originalFilePath, MediaTiers.ONLINE).flatMap({
      case Some(pendingDeletionRecord) =>
        getOnlineSize(pendingDeletionRecord, archivedRecord.vidispineItemId).flatMap(sizeMaybe => {
          val (fileSize, objectKey, vsItemId) =
            validateNeededFields(sizeMaybe, Some(pendingDeletionRecord.originalFilePath), pendingDeletionRecord.vidispineItemId)
          val checksumMaybeFut = NOT_IMPL_getChecksumForOnline(vsItemId)
          checksumMaybeFut.flatMap(checksumMaybe => {
            mediaExistsInDeepArchive(checksumMaybe, fileSize, objectKey).flatMap({
              case true =>
                for {
                  mediaRemovedMsg <- deleteMediaFromOnline(pendingDeletionRecord)
                  _ <- removeDeletionPending(pendingDeletionRecord)
                } yield mediaRemovedMsg
              case false =>
                pendingDeletionRecordDAO.updateAttemptCount(pendingDeletionRecord.id.get, pendingDeletionRecord.attempt + 1)
                NOT_IMPL_outputDeepArchiveCopyRequired(pendingDeletionRecord)
            })
          })
        })
      case None =>
        throw SilentDropMessage(Some(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}"))
    })


  def handleDeepArchiveCompleteOrReplayedForNearline(vault: Vault, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
    pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(archivedRecord.originalFilePath, MediaTiers.NEARLINE).flatMap({
      case Some(pendingDeletionRecord) =>
        pendingDeletionRecord.nearlineId match {
          case Some(nearlineId) =>
            val doesMediaExist =
              for {
                // We fetch the current size, because we don't know how old the message is
                nearlineFileSize <- Future.fromTry(Try {vault.getObject(nearlineId)}.map(MetadataHelper.getFileSize))
                (fileSize, objectKey, nearlineId) <-
                  Future(validateNeededFields(Some(nearlineFileSize), Some(pendingDeletionRecord.originalFilePath), pendingDeletionRecord.nearlineId))
                checksumMaybe <- getChecksumForNearline(vault, nearlineId)
                doesMediaExist <- mediaExistsInDeepArchive(checksumMaybe, fileSize, objectKey)
              } yield doesMediaExist

            doesMediaExist.flatMap({
              case true =>
                for {
                  result <- deleteMediaFromNearline(vault, pendingDeletionRecord.mediaTier.toString, Some(pendingDeletionRecord.originalFilePath), pendingDeletionRecord.nearlineId, pendingDeletionRecord.vidispineItemId)
                  _ <- removeDeletionPending(pendingDeletionRecord)
                } yield result
              case false =>
                pendingDeletionRecordDAO.updateAttemptCount(pendingDeletionRecord.id.get, pendingDeletionRecord.attempt + 1)
                NOT_IMPL_outputDeepArchiveCopyRequired(pendingDeletionRecord)
            })
          case None =>
            logger.warn(s"Could not get nearline filesize from application for media ${archivedRecord.originalFilePath}")
            Future(Left(s"Could not get nearline filesize from application for media ${archivedRecord.originalFilePath}"))
        }
      case None =>
        logger.debug(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}")
        throw SilentDropMessage(Some(s"ignoring archive confirmation, no pending deletion for this ${MediaTiers.NEARLINE} item with ${archivedRecord.originalFilePath}"))
    })


  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    routingKey match {
      case "storagetier.restorer.media_not_required.nearline" =>
        msg.as[OnlineOutputMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"))
          case Right(onlineOutputMessageNearline) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleNearlineMediaNotRequired(nearlineVault, internalArchiveVault, onlineOutputMessageNearline)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }

      case "storagetier.restorer.media_not_required.online" =>
        msg.as[OnlineOutputMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a OnlineOutputMessage: $err"))
          case Right(onlineOutputMessageOnline) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleOnlineMediaNotRequired(nearlineVault, internalArchiveVault, onlineOutputMessageOnline)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }

      case "storagetier.nearline.internalarchive.success" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a NearlineRecord: $err"))
          case Right(nearlineRecord) =>
            // TODO somehow handle the fact that we need to check both NEARLINE and ONLINE pending deletion records
            // TODO alt 1: duplicate and send to ourselves; add/split into an OwnMessageProcessor
            // TODO alt 2: have two projects, one 'online_media_remover', one 'nearline_media_remover'
            // TODO alt 3: tbd
            Future.failed(new RuntimeException(s"Reacting to routingKey 'storagetier.nearline.internalarchive.success' not fully implemented yet"))
        }

      case "storagetier.nearline.internalarchive.success.nearline" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a NearlineRecord: $err"))
          case Right(nearlineRecord) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
                  val nearlineVault = vaults.head
                  val internalArchiveVault = vaults(1)
                  handleInternalArchiveCompleteForNearline(nearlineVault, internalArchiveVault, nearlineRecord)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }

      case "storagetier.nearline.internalarchive.success.online" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a NearlineRecord: $err"))
          case Right(nearlineRecord) =>
            mxsConfig.internalArchiveVaultId match {
              case Some(internalArchiveVaultId) =>
                matrixStoreBuilder.withVaultFuture(internalArchiveVaultId) { internalArchiveVault =>
                  handleInternalArchiveCompleteForOnline(internalArchiveVault, nearlineRecord)
                }
              case None =>
                logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
                Future.failed(new RuntimeException(s"Internal archive vault not configured"))
            }
        }


      // GP-786 Deep Archive complete handler
      // GP-787 Archive complete handler
      case key if key == "storagetier.onlinearchive.mediaingest" || key == "storagetier.onlinearchive.replayed" =>
        msg.as[ArchivedRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
            // TODO somehow handle the fact that we need to check both NEARLINE and ONLINE pending deletion records
            // TODO alt 1: duplicate and send to ourselves; add/split into an OwnMessageProcessor
            // TODO alt 2: have two projects, one 'online_media_remover', one 'nearline_media_remover'
            // TODO alt 3: tbd
            Future.failed(new RuntimeException(s"Reacting to routingKey '$key' not fully implemented yet"))
        }

      case key if key == "storagetier.onlinearchive.mediaingest.nearline" || key == "storagetier.onlinearchive.replayed.nearline" =>
        msg.as[ArchivedRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
              handleDeepArchiveCompleteOrReplayedForNearline(vault, archivedRecord)
            }
        }

      case key if key == "storagetier.onlinearchive.mediaingest.online" || key == "storagetier.onlinearchive.replayed.online" =>
        msg.as[ArchivedRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a ArchivedRecord: $err"))
          case Right(archivedRecord) =>
              handleDeepArchiveCompleteOrReplayedForOnline(archivedRecord)
        }

      case _ =>
        logger.warn(s"Dropping message $routingKey from project-restorer exchange as I don't know how to handle it.")
        Future.failed(new RuntimeException(s"Routing key $routingKey dropped because I don't know how to handle it"))
    }


  protected def openMxsObject(vault:Vault, oid:String) = Try { vault.getObject(oid) }

  /**
   * If the given file already exists in the MXS vault (i.e., there is a file with a matching MXFS_PATH _and_ checksum
   * _and_ file size, then a Future(true) is returned
   * If there is a file with matching MXFS_PATH but checksum and/or size do not match, then a Future(false) is returned
   * We demand that the `maybeLocalChecksum` actually has a value, otherwise we will return Future(false) and log a
   * warning
   *
   * @param vault    Vault to check
   * @param fileName file name to check on the Vault
   * @param filePath the filepath of the item to check
   * */
  def existsInTargetVaultWithMd5Match(vault: Vault, fileName: String, filePath: String, fileSize: Long, maybeLocalChecksum: Option[String]): Future[Boolean] = {
    (for {
      potentialMatches <- findMatchingFilesOnVault(vault, filePath, fileSize)
      potentialMatchesFiles <- Future.sequence(potentialMatches.map(entry => Future.fromTry(openMxsObject(vault, entry.oid))))
      alreadyExists <- verifyChecksumMatchUsingChecker(filePath, potentialMatchesFiles, maybeLocalChecksum)
      result <- alreadyExists match {
        case Some(existingId) =>
          logger.info(s"$filePath: Object exists with object id $existingId")
          Future(true)
        case None =>
          logger.info(s"$filePath: Out of ${potentialMatches.length} remote matches, none matched the checksum")
          Future(false)
      }
    } yield result).recoverWith({
      case err: Throwable =>
        logger.warn(s"Could not verify checksum for file $filePath: ${err.getMessage}")
        Future(false)
    })
  }


  /**
   * Checks to see if any of the MXS files in the `potentialFiles` list are a checksum match for `filePath`.
   * Stops and returns the ID of the first match if it finds one, or returns None if there were no matches.
   *
   * @param filePath           local file that is being backed up
   * @param potentialFiles     potential backup copies of this file
   * @param maybeLocalChecksum stored local checksum; if set this is used instead of re-calculating. Leave this out when calling.
   * @return a Future containing the OID of the first matching file if present or None otherwise
   * @throws RuntimeException if the `maybeLocalChecksum` is None
   */
  protected def verifyChecksumMatchUsingChecker(filePath: String, potentialFiles: Seq[MxsObject], maybeLocalChecksum: Option[String]): Future[Option[String]] = {
    val verifiedMaybeLocalChecksum: Option[String] = maybeLocalChecksum match {
      case None => throw new RuntimeException("Must be called with Some(actualChecksum)")
      case Some(value) => Some(value)
    }
    checksumChecker.verifyChecksumMatch(Paths.get(filePath), potentialFiles, verifiedMaybeLocalChecksum)
  }


  protected def callFindByFilenameNew(vault:Vault, fileName:String) = MatrixStoreHelper.findByFilenameNew(vault, fileName)
  protected def callObjectMatrixEntryFromOID(vault:Vault, fileName:String) = ObjectMatrixEntry.fromOID(fileName, vault)

  /**
   * Searches the given vault for files matching the given specification.
   * Both the name and fileSize must match in order to be considered valid.
   * @param vault vault to search
   * @param filePath Path representing the file path to look for.
   * @param fileSize Long representing the size of the file to match
   * @return a Future, containing a sequence of ObjectMatrixEntries that match the given file path and size
   */
//  def findMatchingFilesOnNearline_fromFileCopier(vault: Vault, filePath: Path, fileSize: Long) = {
  def findMatchingFilesOnVault(vault: Vault, filePath: String, fileSize: Long) = {
    logger.debug(s"Looking for files matching $filePath at size $fileSize")
    callFindByFilenameNew(vault, filePath)
      .map(fileNameMatches=>{
        val nullSizes = fileNameMatches.map(_.maybeGetSize()).collect({case None=>None}).length
        // TODO Decide if we need to/should do this? Flip it around maybe?
        if(nullSizes>0) {
          throw new BailOutExceptionMR(s"Could not check for matching files of $filePath because $nullSizes / ${fileNameMatches.length} had no size")
        }

        val sizeMatches = fileNameMatches.filter(_.maybeGetSize().contains(fileSize))
        logger.debug(s"$filePath: ${fileNameMatches.length} files matched name and ${sizeMatches.length} matched size")
        logger.debug(fileNameMatches.map(obj=>s"${obj.pathOrFilename.getOrElse("-")}: ${obj.maybeGetSize()}").mkString("; "))
        sizeMatches
      })
  }


  def getChecksumForNearline(vault: Vault, oid: String): Future[Option[String]] = {
    for {
      mxsFile <- Future.fromTry(openMxsObject(vault, oid))
      maybeMd5 <- MatrixStoreHelper.getOMFileMd5(mxsFile).flatMap({
            case Failure(err) =>
              logger.error(s"Unable to get checksum from appliance, file should be considered unsafe", err)
              Future(None)
            case Success(remoteChecksum) =>
              logger.info(s"Appliance reported checksum of $remoteChecksum")
              Future(Some(remoteChecksum))
          })
      } yield maybeMd5
  }


  def NOT_IMPL_getChecksumForOnline(vsItemId: String): Future[Option[String]] = {
    // TODO? How to get the -- md5 -- checksum for a vidispine item? VS defaults to SHA-1 unless overridden in configuration property 'fileHashAlgorithm'
    ???
  }


  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, filePath, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    nearlineExistsInInternalArchive(vault, internalArchiveVault, nearlineId, filePath, fileSize)
  }


  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, nearlineId: String, filePath: String, fileSize: Long): Future[Boolean] = {
    for {
      maybeChecksum <- getChecksumForNearline(vault, nearlineId)
      exists <- existsInTargetVaultWithMd5Match(internalArchiveVault, filePath, filePath, fileSize, maybeChecksum)
    } yield exists
  }


  def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] =
    for {
      maybeChecksum <- NOT_IMPL_getChecksumForOnline(vsItemId)
      exists <- existsInTargetVaultWithMd5Match(nearlineVaultOrInternalArchiveVault, filePath, filePath, fileSize, maybeChecksum)
    } yield exists


  def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, objectKey, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    val maybeChecksumFut = getChecksumForNearline(vault, nearlineId)
    maybeChecksumFut.flatMap(checksumMaybe => mediaExistsInDeepArchive(checksumMaybe, fileSize, objectKey))
  }


  def mediaExistsInDeepArchive(checksumMaybe: Option[String], fileSize: Long, objectKey: String): Future[Boolean] = {
    s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(objectKey, fileSize, checksumMaybe).map({
      case true =>
        logger.info(s"File with objectKey $objectKey and size $fileSize exists, safe to delete from higher level")
        true
      case false =>
        logger.info(s"No file $objectKey with matching size $fileSize found, do not delete")
        false
    }).recover({
      case err:Throwable =>
        logger.warn(s"Could not connect to deep archive to check if media exists, do not delete. Err: ${err.getMessage}")
        false
    })
  }


  def removeDeletionPending(existingRecord: PendingDeletionRecord): Future[Int] =
    pendingDeletionRecordDAO.deleteRecord(existingRecord)


  def removeDeletionPendingByMessage(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    (msg.mediaTier, msg.vidispineItemId, msg.nearlineId) match {
      case ("NEARLINE", _, Some(nearlineId)) =>
        pendingDeletionRecordDAO
          .findByNearlineId(nearlineId)
          .flatMap({
            case Some(existingRecord)=>
              logger.debug(s"Deleting pendingDeletionRecord ${existingRecord.id.getOrElse(-1)} for ${msg.mediaTier}, oid ${msg.nearlineId}")
              pendingDeletionRecordDAO.deleteRecord(existingRecord).map(i => Right(i))
            case None=>
              Future(Left("Should not happen"))
          })
      case ("NEARLINE", _, _) =>
        Future.failed(new RuntimeException("NEARLINE but no nearlineId"))

      case ("ONLINE", Some(vsItemId), _) =>
        logger.warn(s"Not implemented yet - $vsItemId ignored")
        Future.failed(SilentDropMessage(Some(s"Not implemented yet - $vsItemId ignored")))
      case ("ONLINE", _, _) =>
        Future.failed(new RuntimeException("ONLINE but no itemId"))

      case (_, _, _) =>
        Future.failed(new RuntimeException("This should not happen!"))
    }


  def deleteSingleAttMedia(vault: Vault, objectMatrixEntry: ObjectMatrixEntry, attributeKey: String, nearlineIdForLog: String, filePathForLog: String): Future[Either[String, Boolean]] =
    objectMatrixEntry.stringAttribute(attributeKey) match {
      case None =>
        logger.info(s"No $attributeKey to remove for main nearline media oid=$nearlineIdForLog, path=$filePathForLog")
        Future(Right(false))
      case Some(attOid) =>
        Try { vault.getObject(attOid).delete() } match {
          case Success(_) =>
            logger.info(s"$attributeKey with oid $attOid removed for main nearline media oid=$nearlineIdForLog, path=$filePathForLog")
            Future(Right(true))
          case Failure(exception) =>
            logger.warn(s"Failed to remove nearline media oid=$nearlineIdForLog, path=$filePathForLog, reason: ${exception.getMessage}")
            Future(Left(s"Failed to remove $attributeKey with oid=$attOid for main nearline media oid=$nearlineIdForLog, path=$filePathForLog, reason: ${exception.getMessage}"))
        }
    }


  def dealWithAttFiles(vault: Vault, nearlineId: String, originalFilePath: String): Future[Either[String, String]] = {
    val combinedRes = for {
      objectMatrixEntry <- callObjectMatrixEntryFromOID(vault, nearlineId)
      proxyRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_PROXY_OID", nearlineId, originalFilePath)
      thumbRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_THUMB_OID", nearlineId, originalFilePath)
      metaRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_META_OID", nearlineId, originalFilePath)
    } yield (proxyRes, thumbRes, metaRes)

    combinedRes.map {
      case (Right(_), Right(_), Right(_)) => Right("Smooth sailing, ATT files removed")
      case (_, _, _) => Left("One or more ATT files could not be removed. Please look at logs for more info")
    }
  }


  def deleteMediaFromNearline(vault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]): Future[Either[String, MessageProcessorReturnValue]] =
    (mediaTier, filePathMaybe, nearlineIdMaybe) match {
      case ("NEARLINE", Some(filepath), Some(nearlineId)) =>
        dealWithAttFiles(vault, nearlineId, filepath)
        // TODO do we need to wrap this with a Future.fromTry?
        Try { vault.getObject(nearlineId).delete() } match {
          case Success(_) =>
            logger.info(s"Nearline media oid=$nearlineId, path=$filepath removed")
            Future(Right(MediaRemovedMessage(mediaTier = mediaTier, originalFilePath = filepath, nearlineId = Some(nearlineId), vidispineItemId = vidispineItemIdMaybe).asJson))
          case Failure(exception) =>
            logger.warn(s"Failed to remove nearline media oid=$nearlineId, path=$filepath, reason: ${exception.getMessage}")
            Future(Left(s"Failed to remove nearline media oid=$nearlineId, path=$filepath, reason: ${exception.getMessage}"))
        }
      case (_, _, _) => throw new RuntimeException(s"Cannot delete from nearline, wrong media tier ($mediaTier), or missing nearline id ($nearlineIdMaybe)")
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
            logger.info(s"Online media item id=$vsItemId removed, path=$filePathMaybe removed")
            Right(MediaRemovedMessage(mediaTier, filePathMaybe.getOrElse("<missing file path>"), vidispineItemIdMaybe, nearlineIdMaybe).asJson)
          })
          .recover({
            case err: Throwable =>
              logger.warn(s"Failed to remove online media id=$vsItemId, reason: ${err.getMessage}")
              Left(s"Failed to remove online media id=$vsItemId, reason: ${err.getMessage}")
          })
      case (_, _) => throw new RuntimeException(s"Cannot delete from online, wrong media tier ($mediaTier), or missing item id (${vidispineItemIdMaybe.getOrElse("<missing item id>")})")
    }


  def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = ???
  def NOT_IMPL_outputDeepArchiveCopyRequired(pendingDeletionRecord: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] = ???
  def NOT_IMPL_outputInternalArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = ???
  def NOT_IMPL_outputNearlineCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = ???
  def NOT_IMPL_outputInternalArchiveCopyRequired(pendingDeletionRecord: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] = ???


  def storeDeletionPending(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    msg.originalFilePath match {
      case Some(filePath) =>
        Try { MediaTiers.withName(msg.mediaTier) } match {
          case Success(tier) => // FIXME needs to discriminate based on mediatier and respective media id
            pendingDeletionRecordDAO
              .findBySourceFilenameAndMediaTier(filePath, tier)
              .map({
                case Some(existingRecord) => existingRecord.copy(attempt = existingRecord.attempt + 1)
                case None =>
                  PendingDeletionRecord(None, originalFilePath = filePath, nearlineId = msg.nearlineId, vidispineItemId = msg.vidispineItemId, mediaTier = tier, attempt = 1)
              })
              .flatMap(rec=>{
                pendingDeletionRecordDAO
                  .writeRecord(rec)
                  .map(recId => Right(recId))
              })
          case Failure(ex) =>
            logger.warn(s"Unexpected value for MediaTier: ${ex.getMessage}")
            Future.failed(new RuntimeException(s"Cannot store PendingDeletion, unexpected value for mediaTier: '${msg.mediaTier}"))
        }
      case None =>
        logger.warn(s"No filepath for ${msg.asJson}, no use storing a PendingDeletion; dropping message")
        Future.failed(new RuntimeException("Cannot store PendingDeletion record for item without filepath"))
    }


  def getActionToPerformOnline(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]): (Action.Value, Option[ProjectRecord]) =
    maybeProject match {
      case None => (Action.DropMsg, None)
      case Some(project) =>
        project.status match {
          case status if status == EntryStatus.Held =>
            (Action.CheckNearline, Some(project))
          case _ =>
            project.deletable match {
              case Some(true) =>
                project.status match {
                  case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                    // deletable + Completed/Killed
                    (Action.ClearAndDelete, Some(project))
                  case _ =>
                    (Action.DropMsg, Some(project))
                }
              case _ =>
                // not DELETABLE
                if (project.deep_archive.getOrElse(false)) {
                  if (project.sensitive.getOrElse(false)) {
                    if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                      // not deletable + deep_archive + sensitive + Completed/Killed
                      (Action.CheckInternalArchive, Some(project))
                    } else {
                      (Action.DropMsg, Some(project))
                    }
                  } else {
                    if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                      // not deletable + deep_archive + not sensitive + Completed/Killed
                      (Action.CheckDeepArchiveForOnline, Some(project))
                    } else {
                      (Action.DropMsg, Some(project))
                    }
                  }
                } else {
                  // not deletable + not deep_archive
                  (Action.JustNo, Some(project))
                }
            }
        }
    }


  def getActionToPerformNearline(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]): (Action.Value, Option[ProjectRecord]) =
    maybeProject match {
      case None => (Action.DropMsg, None)
      case Some(project) =>
        project.deletable match {
          case Some(true) =>
            project.status match {
              case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                onlineOutputMessage.mediaCategory.toLowerCase match {
                  case "deliverables" => (Action.DropMsg, Some(project))
                  case _ => (Action.ClearAndDelete, Some(project))
                }
              case _ => (Action.DropMsg, Some(project))
            }
          case _ =>
          // not DELETABLE
          if (project.deep_archive.getOrElse(false)) {
            if (project.sensitive.getOrElse(false)) {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                (Action.CheckInternalArchive, Some(project))
              } else {
                (Action.DropMsg, Some(project))
              }
            } else {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                (Action.CheckDeepArchiveForNearline, Some(project))
              } else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                (Action.DropMsg, Some(project))
              }
            }
          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            (Action.JustNo, Some(project))
          }
        }
    }


  private def performActionNearline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] =
    actionToPerform match {
      case (Action.DropMsg, None) =>
        handleDropMsg(onlineOutputMessage)

      case (Action.DropMsg, Some(project)) =>
        handleDropMsg(onlineOutputMessage, project)

      case (Action.CheckDeepArchiveForNearline, Some(project)) =>
        handleCheckDeepArchiveForNearline(nearlineVault, onlineOutputMessage, project)

      case (Action.CheckInternalArchive, Some(project)) =>
        handleCheckInternalArchiveForNearline(nearlineVault, internalArchiveVault, onlineOutputMessage, project)

      case (Action.ClearAndDelete, Some(project)) =>
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)

      case (Action.JustNo, Some(project)) =>
        handleJustNo(project)

      case (unexpectedAction, Some(project)) =>
        handleUnexpectedAction(unexpectedAction, project)

      case (unexpectedAction, _) =>
        handleUnexpectedAction(unexpectedAction)
    }


  private def handleUnexpectedAction(unexpectedAction: MediaNotRequiredMessageProcessor.Action.Value) = {
    logger.warn(s"Cannot remove file: unexpected action $unexpectedAction when no project")
    throw new RuntimeException(s"Cannot remove file: unexpected action $unexpectedAction when no project")
  }

  private def handleUnexpectedAction(unexpectedAction: MediaNotRequiredMessageProcessor.Action.Value, project: ProjectRecord) = {
    logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $unexpectedAction")
    throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, unexpected action $unexpectedAction")
  }

  private def handleDropMsg(onlineOutputMessage: OnlineOutputMessage) = {
    val noProjectFoundMsg = s"No project could be found that is associated with $onlineOutputMessage, erring on the safe side, not removing"
    logger.warn(noProjectFoundMsg)
    throw SilentDropMessage(Some(noProjectFoundMsg))
  }


  private def handleCheckDeepArchiveForNearline(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    nearlineMediaExistsInDeepArchive(nearlineVault, onlineOutputMessage).flatMap({
      case true =>
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)
      case false =>
        storeDeletionPending(onlineOutputMessage)
        NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage)
    })
  }


  private def handleCheckInternalArchiveForNearline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    nearlineExistsInInternalArchive(nearlineVault, internalArchiveVault, onlineOutputMessage).flatMap({
      case true =>
        // nearline media EXISTS in INTERNAL ARCHIVE
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)
      case false =>
        // nearline media does NOT EXIST in INTERNAL ARCHIVE
        storeDeletionPending(onlineOutputMessage)
        NOT_IMPL_outputInternalArchiveCopyRequired(onlineOutputMessage)
    })
  }


  private def handleDeleteNearlineAndClear(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    for {
      mediaRemovedMsg <- deleteFromNearlineWrapper(nearlineVault, onlineOutputMessage, project)
      _ <- removeDeletionPendingByMessage(onlineOutputMessage)
    } yield mediaRemovedMsg
  }


  private def handleJustNo(project: ProjectRecord) = {
    logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
    throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
  }


  def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, originalFilePath, vsItemId) =
      validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)

    for {
      checksumMaybe <- NOT_IMPL_getChecksumForOnline(vsItemId)
      mediaExists <- mediaExistsInDeepArchive(checksumMaybe, fileSize, originalFilePath)
    } yield mediaExists
  }


  private def handleCheckVaultForOnline(nearlineVaultOrInternalArchiveVault: Vault,
                                        onlineOutputMessage: OnlineOutputMessage,
                                        outputRequiredMsgFn: OnlineOutputMessage => Future[Either[String, MessageProcessorReturnValue]]
                                       ) = {
    val (fileSize, filePath, vsItemId) =
      validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)
    onlineExistsInVault(nearlineVaultOrInternalArchiveVault, vsItemId, filePath, fileSize).flatMap({
        case true => handleDeleteOnlineAndClear(onlineOutputMessage)
        case false =>
          storeDeletionPending(onlineOutputMessage)
          outputRequiredMsgFn(onlineOutputMessage)
      })
  }


  private def handleDeleteOnlineAndClear(onlineOutputMessage: OnlineOutputMessage) = {
    for {
      mediaRemovedMessage <- deleteMediaFromOnline(onlineOutputMessage)
      _ <- removeDeletionPendingByMessage(onlineOutputMessage)
    } yield mediaRemovedMessage
  }


  private def handleCheckDeepArchiveForOnline(onlineOutputMessage: OnlineOutputMessage) =
    onlineMediaExistsInDeepArchive(onlineOutputMessage).flatMap({
      case true =>
        handleDeleteOnlineAndClear(onlineOutputMessage)
      case false =>
        storeDeletionPending(onlineOutputMessage)
        NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage)
    })


  private def handleDropMsg(onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    val deletable = project.deletable.getOrElse(false)
    val deep_archive = project.deep_archive.getOrElse(false)
    val sensitive = project.sensitive.getOrElse(false)
    val nearlineId = onlineOutputMessage.nearlineId.getOrElse("-1")
    val vsItemId = onlineOutputMessage.vidispineItemId.getOrElse("<missing item id>")
    val notRemovingMsg = s"Not removing ${onlineOutputMessage.mediaTier} media with " +
      s"nearlineId=$nearlineId, " +
      s"onlineId=$vsItemId: " +
      s"project (id ${project.id.getOrElse(-1)}) is " +
      s"deletable($deletable), " +
      s"deep_archive($deep_archive), " +
      s"sensitive($sensitive), " +
      s"status is ${project.status}, " +
      s"media category is ${onlineOutputMessage.mediaCategory}."
    logger.debug(s"-> $notRemovingMsg")
    throw SilentDropMessage(Some(notRemovingMsg))
  }


  def deleteFromNearlineWrapper(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    deleteMediaFromNearline(nearlineVault, onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId, onlineOutputMessage.vidispineItemId).map({
      case Left(err) => Left(err)
      case Right(mediaRemovedMessage) =>
        logger.debug(s"--> deleting nearline media ${onlineOutputMessage.nearlineId} for project ${project.id.getOrElse(-1)}")
        Right(mediaRemovedMessage.asJson)
    })
  }


  def handleNearlineMediaNotRequired(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    for {
      /* ignore all but the first project - we're only getting the main project as of yet */
      projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head)
      actionToPerform <- Future(getActionToPerformNearline(onlineOutputMessage, projectRecordMaybe))
      fileRemoveResult <- performActionNearline(nearlineVault, internalArchiveVault, onlineOutputMessage, actionToPerform)
    } yield fileRemoveResult
  }


  def validateNeededFields(fileSizeMaybe: Option[Long], filePathMaybe: Option[String], nearlineOrOnlineIdMaybe: Option[String]): (Long, String, String) = {
    (fileSizeMaybe, filePathMaybe, nearlineOrOnlineIdMaybe) match {
      case (None, _, _) => throw new RuntimeException(s"fileSize is missing")
      case (_, None, _) => throw new RuntimeException(s"filePath is missing")
      case (_, _, None) => throw new RuntimeException(s"media id is missing")
      case (Some(-1), _, _) => throw new RuntimeException(s"fileSize is -1")
      case (Some(fileSize), Some(filePath), Some(id)) => (fileSize, filePath, id)
    }
  }


  private def handleOnlineMediaNotRequired(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    // Sanity checks
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)
    for {
      /* ignore all but the first project - we're only getting the main project as of yet */
      projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head)
      actionToPerform <- Future(getActionToPerformOnline(onlineOutputMessage, projectRecordMaybe))
      fileRemoveResult <- performActionOnline(vault, internalArchiveVault, onlineOutputMessage, actionToPerform)
    } yield fileRemoveResult

    Future(Left("testing online"))
  }

  def performActionOnline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (MediaNotRequiredMessageProcessor.Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] = {
    actionToPerform match {
      case (Action.CheckDeepArchiveForOnline, Some(project)) =>
        handleCheckDeepArchiveForOnline(onlineOutputMessage)

      case (Action.CheckInternalArchive, Some(project)) =>
        handleCheckVaultForOnline(internalArchiveVault, onlineOutputMessage, NOT_IMPL_outputInternalArchiveCopyRequired)

      case (Action.CheckNearline, Some(project)) =>
        handleCheckVaultForOnline(nearlineVault, onlineOutputMessage, NOT_IMPL_outputNearlineCopyRequired)

      case (Action.ClearAndDelete, Some(project)) =>
        handleDeleteOnlineAndClear(onlineOutputMessage)

      case (Action.DropMsg, None) =>
        handleDropMsg(onlineOutputMessage)

      case (Action.DropMsg, Some(project)) =>
        handleDropMsg(onlineOutputMessage, project)

      case (Action.JustNo, Some(project)) =>
        handleJustNo(project)

      case (unexpectedAction, Some(project)) =>
        handleUnexpectedAction(unexpectedAction, project)

      case (unexpectedAction, _) =>
        handleUnexpectedAction(unexpectedAction)
    }
  }

}


object MediaNotRequiredMessageProcessor {
  object Action extends Enumeration {
    val CheckDeepArchiveForNearline, CheckInternalArchive, ClearAndDelete, DropMsg, JustNo, CheckNearline, CheckDeepArchiveForOnline  = Value
  }
}
