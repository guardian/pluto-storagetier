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
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VidispineCommunicator}
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


  def getMd5ChecksumForOnline(vsItemId: String): Future[Option[String]] =
    getOriginalShapeForVidispineItem(vsItemId).map({
      case Left(err) =>
        logger.info(s"Couldn't get MD5 checksum for vsItemId $vsItemId: $err")
        None
      case Right(originalShape) => originalShape.getLikelyFile.flatMap(_.md5Option)
    })


  def getVidispineSize(itemId: String): Future[Either[String, Option[Long]]] =
    getOriginalShapeForVidispineItem(itemId).map({
      case Left(value) => Left(value)
      case Right(originalShape) => Right(originalShape.getLikelyFile.flatMap(_.sizeOption))
    })


  def getOriginalShapeForVidispineItem(itemId: String): Future[Either[String, ShapeDocument]] =
    vidispineCommunicator.listItemShapes(itemId).map({
      case None =>
        logger.error(s"Can't get original shape for vidispine item $itemId as it has no shapes on it")
        Left(s"Can't get original shape for vidispine item $itemId as it has no shapes on it")
      case Some(shapes) =>
        shapes.find(_.tag.contains("original")) match {
          case None =>
            logger.error(s"Can't get original shape for of vidispine item $itemId. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
            Left(s"Can't get original shape for of vidispine item $itemId. Shapes were: ${shapes.flatMap(_.tag).mkString("; ")}")
          case Some(originalShape: ShapeDocument) =>
            Right(originalShape)
        }
    })


  def handleInternalArchiveCompleteForNearline(vault: Vault, internalArchiveVault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] =
    pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(nearlineRecord.objectId).flatMap({
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


  def handleInternalArchiveCompleteForOnline(internalArchiveVault: Vault, nearlineRecord: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] =
    pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(nearlineRecord.objectId).flatMap({
      case Some(pendingDeletionRecord) =>
        pendingDeletionRecord.vidispineItemId match {
          case Some(vsItemId) =>
              getOnlineSize(pendingDeletionRecord, nearlineRecord.vidispineItemId).flatMap({
                case Some(onlineSize) =>
                  val filePathBack = putItBack(pendingDeletionRecord.originalFilePath)
                  onlineExistsInVault(internalArchiveVault, vsItemId, filePathBack, onlineSize).flatMap({
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


  def handleDeepArchiveCompleteOrReplayedForOnline(archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    val (_,_,vsItemId) = validateNeededFields(Some(1), Some("path"), archivedRecord.vidispineItemId)
    pendingDeletionRecordDAO.findByOnlineIdForONLINE(vsItemId).flatMap({
      case Some(pendingDeletionRecord) =>
        getOnlineSize(pendingDeletionRecord, archivedRecord.vidispineItemId).flatMap(sizeMaybe => {
          val (fileSize, objectKey, vsItemId) =
            validateNeededFields(sizeMaybe, Some(pendingDeletionRecord.originalFilePath), pendingDeletionRecord.vidispineItemId)
          val checksumMaybeFut = getMd5ChecksumForOnline(vsItemId)
          checksumMaybeFut.flatMap(checksumMaybe => {
            mediaExistsInDeepArchive(MediaTiers.ONLINE.toString, checksumMaybe, fileSize, objectKey).flatMap({
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
  }


  def handleDeepArchiveCompleteOrReplayedForNearline(vault: Vault, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    // FIXME It seems we do not have nearlineId in an archiveRecord, and as we've previously noticed, originalFilePath can be the same for more than one nearline media item
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
                doesMediaExist <- mediaExistsInDeepArchive(MediaTiers.NEARLINE.toString, checksumMaybe, fileSize, objectKey)
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
  }


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
//   * @param fileName file name to check on the Vault
   * @param filePath the filepath of the item to check
   * */
  def existsInTargetVaultWithMd5Match(mediaTier: MediaTiers.Value, id: String, vault: Vault, fileName: String, filePath: String, fileSize: Long, maybeLocalChecksum: Option[String]): Future[Boolean] = {
    val wantedFileInfo = s"$filePath ($id, $fileSize, ${maybeLocalChecksum.getOrElse("<no local checksum>")})"
    (for {
      potentialMatches <- findMatchingFilesOnVault(mediaTier, vault, filePath, fileSize)
      potentialMatchesFiles <- Future.sequence(potentialMatches.map(entry => Future.fromTry(openMxsObject(vault, entry.oid))))
      alreadyExists <- verifyChecksumMatchUsingChecker(filePath, potentialMatchesFiles, maybeLocalChecksum)
      result <- alreadyExists match {
        case Some(existingId) =>
          logger.info(s"$wantedFileInfo: Copy exists on ${vault.getId} with object id $existingId")
          Future(true)
        case None =>
          if (potentialMatches.isEmpty)
            logger.info(s"$wantedFileInfo: No remote name matches, nothing to compare checksum for")
          else
            logger.info(s"$wantedFileInfo: No checksum match for any of the ${potentialMatches.length} remote name matches")
          Future(false)
      }
    } yield result).recoverWith({
      case err: Throwable =>
        logger.warn(s"Could not verify checksum for file $wantedFileInfo: ${err.getMessage}")
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
  def findMatchingFilesOnVault(mediaTier: MediaTiers.Value, vault: Vault, filePath: String, fileSize: Long): Future[Seq[ObjectMatrixEntry]] = {
    logger.debug(s"Looking for files matching $mediaTier media with $filePath at size $fileSize on ${vault.getId}")
    callFindByFilenameNew(vault, filePath)
      .map(fileNameMatches => {
        val nullSizes = fileNameMatches.map(_.maybeGetSize()).collect({ case None => None }).length
        // TODO Decide if we need to/should do this? Flip it around maybe?
        if (nullSizes > 0) {
          throw new BailOutExceptionMR(s"Could not check for matching files of $filePath because $nullSizes / ${fileNameMatches.length} had no size")
        }

        val sizeMatches = fileNameMatches.filter(_.maybeGetSize().contains(fileSize))

        if (fileNameMatches.nonEmpty) {
          val str = fileNameMatches.map(obj => s"oid ${obj.oid}, path '${obj.pathOrFilename.getOrElse("-")}', size ${obj.maybeGetSize().get}").mkString("; ")
          logger.debug(s"Object(s) matched on name on ${vault.getId}: $str")
          logger.debug(s"$filePath: ${fileNameMatches.length} files matched name and ${sizeMatches.length} matched size")
        } else {
          logger.debug(s"$filePath: No files matched name on ${vault.getId}")
        }
        sizeMatches
      })
  }


  def getChecksumForNearline(vault: Vault, oid: String): Future[Option[String]] = {
    for {
      mxsFile <- Future.fromTry(openMxsObject(vault, oid))
      maybeMd5 <- MatrixStoreHelper.getOMFileMd5(mxsFile).flatMap({
            case Failure(err) =>
              logger.error(s"Unable to get checksum from appliance ${vault.getId}, file with $oid should be considered unsafe", err)
              Future(None)
            case Success(remoteChecksum) =>
              logger.info(s"Appliance reported checksum of $remoteChecksum for $oid on $vault")
              Future(Some(remoteChecksum))
          })
      } yield maybeMd5
  }


  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, filePath, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    nearlineExistsInInternalArchive(vault, internalArchiveVault, nearlineId, filePath, fileSize)
  }


  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, nearlineId: String, filePath: String, fileSize: Long): Future[Boolean] = {
    val filePathBack = putItBack(filePath)
    for {
      maybeChecksum <- getChecksumForNearline(vault, nearlineId)
      //TODO add nearlineId to parameter list for logging purposes(?)
      exists <- existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, internalArchiveVault, filePathBack, filePathBack, fileSize, maybeChecksum)
    } yield exists
  }


  private def putItBack(filePath: String): String = filePath match {
    case f if f.startsWith("/") =>
      filePath
    case _ => asLookup.putBackBase(Paths.get(filePath)) match {
      case Left(err) =>
        logger.warn(s"Could not but back base path for $filePath: $err. Will use as was.")
        filePath
      case Right(value) =>
        logger.debug(s"$filePath was restored to ${value.toString}")
        value.toString
    }
  }

  def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] =
    for {
      maybeChecksum <- getMd5ChecksumForOnline(vsItemId)
      exists <- existsInTargetVaultWithMd5Match(MediaTiers.ONLINE, vsItemId, nearlineVaultOrInternalArchiveVault, filePath, filePath, fileSize, maybeChecksum)
    } yield exists


  def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, objectKey, nearlineId) = validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    val maybeChecksumFut = getChecksumForNearline(vault, nearlineId)
    maybeChecksumFut.flatMap(checksumMaybe => mediaExistsInDeepArchive(onlineOutputMessage.mediaTier, checksumMaybe, fileSize, objectKey))
  }


  def mediaExistsInDeepArchive(mediaTier: String, checksumMaybe: Option[String], fileSize: Long, originalFilePath: String): Future[Boolean] = {

    val objectKey =
      asLookup.relativizeFilePath(Paths.get(originalFilePath)) match {
        case Left(err) =>
          logger.error(s"Could not relativize $mediaTier file path $originalFilePath: $err. Checking ${originalFilePath.stripPrefix("/")}")
          originalFilePath.stripPrefix("/")
        case Right(relativePath) =>
          relativePath.toString
      }

    s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(objectKey, fileSize, checksumMaybe).map({
      case true =>
        logger.info(s"$mediaTier file with path $originalFilePath, objectKey $objectKey and size $fileSize exists, safe to delete from higher level")
        true
      case false =>
        logger.info(s"$mediaTier file with path $originalFilePath: No file $objectKey with matching size $fileSize found, do not delete")
        false
    }).recover({
      case err:Throwable =>
        logger.warn(s"Could not connect to deep archive to check if copy of $mediaTier media exists, do not delete. Err: ${err.getMessage}")
        false
    })
  }


  def removeDeletionPending(existingRecord: PendingDeletionRecord): Future[Int] =
    pendingDeletionRecordDAO.deleteRecord(existingRecord)


  def removeDeletionPendingByMessage(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    (msg.mediaTier, msg.vidispineItemId, msg.nearlineId) match {
      case ("NEARLINE", _, Some(nearlineId)) =>
        pendingDeletionRecordDAO
          .findByNearlineIdForNEARLINE(nearlineId)
          .flatMap({
            // The number returned in the Right is the number of affected rows.
            // We always call this method when we delete an item, instead of first checking
            // if there exists a pending deletetion => if none is found, that's not an
            // error, but obviously no rows are updated, hence Right(0) in that case.
            case Some(existingRecord)=>
              deleteExistingPendingDeletionRecord(existingRecord)
            case None=>
              Future(Right(0))
          })

      case ("NEARLINE", _, _) =>
        Future.failed(new RuntimeException("NEARLINE but no nearlineId"))

      case ("ONLINE", Some(vsItemId), _) =>
        pendingDeletionRecordDAO
          .findByOnlineIdForONLINE(vsItemId)
          .flatMap({
            case Some(existingRecord)=>
              deleteExistingPendingDeletionRecord(existingRecord)
            case None=>
              Future(Right(0))
          })

      case ("ONLINE", _, _) =>
        Future.failed(new RuntimeException("ONLINE but no vsItemId"))

      case (_, _, _) =>
        Future.failed(new RuntimeException("This should not happen!"))
    }


  private def deleteExistingPendingDeletionRecord(existingRecord: PendingDeletionRecord) =
    pendingDeletionRecordDAO.deleteRecord(existingRecord).map { i =>
      logger.debug(s"Deleted ${getInformativePendingDeletionString(existingRecord)}")
      Right(i)
    }.recover {
      case e: Exception =>
        logger.warn(s"Failed to delete ${getInformativePendingDeletionString(existingRecord)}. Cause: ${e.getMessage}")
        Left(s"Failed to delete ${getInformativePendingDeletionString(existingRecord)}. Cause: ${e.getMessage}")
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
            logger.info(s"Deleted $mediaTier media: itemId $vsItemId, path ${filePathMaybe.getOrElse("<missing file path>")}")
            Right(MediaRemovedMessage(mediaTier, filePathMaybe.getOrElse("<missing file path>"), vidispineItemIdMaybe, nearlineIdMaybe).asJson)
          })
          .recover({
            case err: Throwable =>
              logger.warn(s"Failed to remove $mediaTier media: itemId $vsItemId, path ${filePathMaybe.getOrElse("<missing file path>")}, reason: ${err.getMessage}")
              Left(s"Failed to remove online media itemId $vsItemId, path ${filePathMaybe.getOrElse("<missing file path>")}, reason: ${err.getMessage}")
          })
      case (_, _) => throw new RuntimeException(s"Cannot delete from online, wrong media tier ($mediaTier), or missing item id (${vidispineItemIdMaybe.getOrElse("<missing item id>")})")
    }


  def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    val message = s"outputDeepArchiveCopyRequired not implemented yet (not removing ${onlineOutputMessage.mediaTier} ${onlineOutputMessage.originalFilePath})"
    logger.warn(message)
    throw SilentDropMessage(Some(message))
  }

  def NOT_IMPL_outputDeepArchiveCopyRequired(pendingDeletionRecord: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    val message = s"outputDeepArchiveCopyRequired not implemented yet (not removing ${pendingDeletionRecord.mediaTier} ${pendingDeletionRecord.originalFilePath})"
    logger.warn(message)
    throw SilentDropMessage(Some(message))
  }

  def NOT_IMPL_outputInternalArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    val message = s"outputInternalArchiveCopyRequired not implemented yet (not removing ${onlineOutputMessage.mediaTier} ${onlineOutputMessage.originalFilePath})"
    logger.warn(message)
    throw SilentDropMessage(Some(message))
  }

  def NOT_IMPL_outputInternalArchiveCopyRequired(pendingDeletionRecord: PendingDeletionRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    val message = s"outputInternalArchiveCopyRequired not implemented yet (not removing ${pendingDeletionRecord.mediaTier} ${pendingDeletionRecord.originalFilePath})"
    logger.warn(message)
    throw SilentDropMessage(Some(message))
  }

  def NOT_IMPL_outputNearlineCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    val message = s"outputNearlineCopyRequired not implemented yet (not removing ${onlineOutputMessage.mediaTier} ${onlineOutputMessage.originalFilePath})"
    logger.warn(message)
    throw SilentDropMessage(Some(message))
  }


  def storeDeletionPending(msg: OnlineOutputMessage): Future[Either[String, Int]] =
    msg.originalFilePath match {
      case Some(filePath) =>
        Try {
          MediaTiers.withName(msg.mediaTier)
        } match {
          case Success(mediaTier) =>
            val recordMaybeFut = mediaTier match {
              case MediaTiers.ONLINE =>
                pendingDeletionRecordDAO.findByOnlineIdForONLINE(validateNeededFields(Some(0), Some(""), msg.vidispineItemId)._3)
              case MediaTiers.NEARLINE =>
                pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(validateNeededFields(Some(0), Some(""), msg.nearlineId)._3)
            }
            recordMaybeFut.map({
              case Some(existingRecord) => existingRecord.copy(attempt = existingRecord.attempt + 1)
              case None =>
                PendingDeletionRecord(None, originalFilePath = filePath, nearlineId = msg.nearlineId, vidispineItemId = msg.vidispineItemId, mediaTier = mediaTier, attempt = 1)
            }).flatMap(rec => {
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
      case None =>
        logger.debug(s"Action to perform: '${Action.DropMsg}' for ${getInformativeIdStringNoProject(onlineOutputMessage)}")
        (Action.DropMsg, None)
      case Some(project) =>
        project.status match {
          case status if status == EntryStatus.Held =>
            logAndSelectAction(Action.CheckExistsOnNearline, onlineOutputMessage, project)
          case _ =>
            project.deletable match {
              case Some(true) =>
                project.status match {
                  case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                    // deletable + Completed/Killed
                    logAndSelectAction(Action.ClearAndDelete, onlineOutputMessage, project)
                  case _ =>
                    logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                }
              case _ =>
                // not DELETABLE
                if (project.deep_archive.getOrElse(false)) {
                  if (project.sensitive.getOrElse(false)) {
                    if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                      // not deletable + deep_archive + sensitive + Completed/Killed
                      logAndSelectAction(Action.CheckInternalArchive, onlineOutputMessage, project)
                    } else {
                      logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                    }
                  } else {
                    if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                      // not deletable + deep_archive + not sensitive + Completed/Killed
                      logAndSelectAction(Action.CheckDeepArchiveForOnline, onlineOutputMessage, project)
                    } else {
                      logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                    }
                  }
                } else {
                  // not deletable + not deep_archive
                  logAndSelectAction(Action.JustNo, onlineOutputMessage, project)
                }
            }
        }
    }


  private def logAndSelectAction(action: MediaNotRequiredMessageProcessor.Action.Value, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    logger.debug(s"Action to perform: '$action' for ${getInformativeIdString(onlineOutputMessage, project)}")
    (action, Some(project))
  }

  def getActionToPerformNearline(onlineOutputMessage: OnlineOutputMessage, maybeProject: Option[ProjectRecord]): (Action.Value, Option[ProjectRecord]) =
    maybeProject match {
      case None =>
        logger.debug(s"Action to perform: '${Action.DropMsg}' for ${getInformativeIdStringNoProject(onlineOutputMessage)}")
        (Action.DropMsg, None)
      case Some(project) =>
        project.deletable match {
          case Some(true) =>
            project.status match {
              case status if status == EntryStatus.Completed || status == EntryStatus.Killed =>
                onlineOutputMessage.mediaCategory.toLowerCase match {
                  case "deliverables" => logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
                  case _ => logAndSelectAction(Action.ClearAndDelete, onlineOutputMessage, project)
                }
              case _ => logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
            }
          case _ =>
          // not DELETABLE
          if (project.deep_archive.getOrElse(false)) {
            if (project.sensitive.getOrElse(false)) {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                logAndSelectAction(Action.CheckInternalArchive, onlineOutputMessage, project)
              } else {
                logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
              }
            } else {
              if (project.status == EntryStatus.Completed || project.status == EntryStatus.Killed) {
                logAndSelectAction(Action.CheckDeepArchiveForNearline, onlineOutputMessage, project)
              } else { // deep_archive + not sensitive + not killed and not completed (GP-785 row 8)
                logAndSelectAction(Action.DropMsg, onlineOutputMessage, project)
              }
            }
          } else {
            // We cannot remove media when the project doesn't have deep_archive set
            logAndSelectAction(Action.JustNo, onlineOutputMessage, project)
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
    val noProjectFoundMsg = s"Dropping request to remove media: No project could be found that is associated with ${getInformativeIdStringNoProject(onlineOutputMessage)}"
    logger.warn(noProjectFoundMsg)
    throw SilentDropMessage(Some(noProjectFoundMsg))
  }


  private def handleCheckDeepArchiveForNearline(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    nearlineMediaExistsInDeepArchive(nearlineVault, onlineOutputMessage).flatMap({
      case true =>
        logger.debug(s"'${getInformativeIdString(onlineOutputMessage, project)}' exists in Deep Archive, going to delete nearline copy")
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)
      case false =>
        logger.debug(s"'${getInformativeIdString(onlineOutputMessage, project)}' does not exist in Deep Archive, going to store deletion pending and request Deep Archive copy")
        storeDeletionPending(onlineOutputMessage)
        NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage)
    })
  }


  private def handleCheckInternalArchiveForNearline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) =
    nearlineExistsInInternalArchive(nearlineVault, internalArchiveVault, onlineOutputMessage).flatMap({
      case true =>
        // nearline media EXISTS in INTERNAL ARCHIVE
        logger.debug(s"Exists in Internal Archive, going to delete ${getInformativeIdString(onlineOutputMessage, project)} ")
        handleDeleteNearlineAndClear(nearlineVault, onlineOutputMessage, project)
      case false =>
        // nearline media DOES NOT EXIST in INTERNAL ARCHIVE
        logger.debug(s"Does not exist in Internal Archive, going to store deletion pending and request Internal Archive copy for ${getInformativeIdString(onlineOutputMessage, project)}")
        storeDeletionPending(onlineOutputMessage)
        NOT_IMPL_outputInternalArchiveCopyRequired(onlineOutputMessage)
    })


  private def handleDeleteNearlineAndClear(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) =
    for {
      mediaRemovedMsg <- deleteFromNearlineWrapper(nearlineVault, onlineOutputMessage, project)
      _ <- removeDeletionPendingByMessage(onlineOutputMessage)
    } yield mediaRemovedMsg


  private def handleJustNo(project: ProjectRecord) = {
    logger.warn(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
    throw new RuntimeException(s"Project state for removing files from project ${project.id.getOrElse(-1)} is not valid, deep_archive flag is not true!")
  }


  def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = {
    val (fileSize, originalFilePath, vsItemId) =
      validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)

    for {
      checksumMaybe <- getMd5ChecksumForOnline(vsItemId)
      mediaExists <- mediaExistsInDeepArchive(onlineOutputMessage.mediaTier, checksumMaybe, fileSize, originalFilePath)
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
    val notRemovingMsg: String = s"Dropping request to remove ${getInformativeIdString(onlineOutputMessage, project)}"
    logger.debug(notRemovingMsg)
    throw SilentDropMessage(Some(notRemovingMsg))
  }


  private def getInformativeIdString(onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord) = {
    s"${onlineOutputMessage.originalFilePath.getOrElse("<missing originalFilePath>")}: " +
      s"${onlineOutputMessage.mediaTier} media with " +
      s"nearlineId ${onlineOutputMessage.nearlineId.getOrElse("<missing nearline ID>")}, " +
      s"onlineId ${onlineOutputMessage.vidispineItemId.getOrElse("<missing vsItem ID>")}, " +
      s"mediaCategory ${onlineOutputMessage.mediaCategory} " +
      s"in project ${project.id.getOrElse(-1)}: " +
      s"deletable(${project.deletable.getOrElse(false)}), " +
      s"deep_archive(${project.deep_archive.getOrElse(false)}), " +
      s"sensitive(${project.sensitive.getOrElse(false)}), " +
      s"status ${project.status}"
  }
  private def getInformativeIdStringNoProject(onlineOutputMessage: OnlineOutputMessage) =
        s"${onlineOutputMessage.mediaTier} media with " +
          s"nearlineId ${onlineOutputMessage.nearlineId.getOrElse("<missing nearline ID>")}, " +
          s"onlineId ${onlineOutputMessage.vidispineItemId.getOrElse("<missing vsItem ID>")}, " +
          s"media category is ${onlineOutputMessage.mediaCategory} - Could not find project record for media"

  private def getInformativePendingDeletionString(project: PendingDeletionRecord) = {
    val id = project.id.getOrElse("<missing rec ID>")
    val nearlineId = project.nearlineId.getOrElse("<missing nearline ID>")
    val vsItemId = project.vidispineItemId.getOrElse("<missing vsItem ID>")

    s"${project.mediaTier} pendingDeletionRecord with " +
      s"id $id, " +
      s"nearlineId $nearlineId, " +
      s"onlineId $vsItemId " +
      s"originalFilePath '${project.originalFilePath}', " +
      s"attempt ${project.attempt}"
  }

  def deleteFromNearlineWrapper(nearlineVault: Vault, onlineOutputMessage: OnlineOutputMessage, project: ProjectRecord): Future[Either[String, MessageProcessorReturnValue]] = {
    deleteMediaFromNearline(nearlineVault, onlineOutputMessage.mediaTier, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId, onlineOutputMessage.vidispineItemId).map({
      case Left(err) =>
        logger.warn(s"Failed to delete ${getInformativeIdString(onlineOutputMessage, project)}. Cause: $err")
        Left(err)
      case Right(mediaRemovedMessage) =>
        logger.debug(s"Deleted ${getInformativeIdString(onlineOutputMessage, project)}")
        Right(mediaRemovedMessage.asJson)
    })
  }


  def handleNearlineMediaNotRequired(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.nearlineId)
    onlineOutputMessage.mediaTier match {
      case "NEARLINE" =>
        for {
          /* ignore all but the first project - we're only getting the main project as of yet */
          projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head)
          actionToPerform <- Future(getActionToPerformNearline(onlineOutputMessage, projectRecordMaybe))
          fileRemoveResult <- performActionNearline(nearlineVault, internalArchiveVault, onlineOutputMessage, actionToPerform)
        } yield fileRemoveResult
      case notWanted =>
        throw new RuntimeException(s"handleNearlineMediaNotRequired called with unexpected mediaTier: $notWanted")
    }
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


  def handleOnlineMediaNotRequired(vault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    // Sanity checks
    validateNeededFields(onlineOutputMessage.fileSize, onlineOutputMessage.originalFilePath, onlineOutputMessage.vidispineItemId)
    onlineOutputMessage.mediaTier match {
      case "ONLINE" =>
        for {
          /* ignore all but the first project - we're only getting the main project as of yet */
          projectRecordMaybe <- asLookup.getProjectMetadata(onlineOutputMessage.projectIds.head)
          actionToPerform <- Future(getActionToPerformOnline(onlineOutputMessage, projectRecordMaybe))
          fileRemoveResult <- performActionOnline(vault, internalArchiveVault, onlineOutputMessage, actionToPerform)
        } yield fileRemoveResult
      case notWanted =>
        throw new RuntimeException(s"handleOnlineMediaNotRequired called with unexpected mediaTier: $notWanted")
    }
  }


  def performActionOnline(nearlineVault: Vault, internalArchiveVault: Vault, onlineOutputMessage: OnlineOutputMessage, actionToPerform: (MediaNotRequiredMessageProcessor.Action.Value, Option[ProjectRecord])): Future[Either[String, MessageProcessorReturnValue]] = {
    actionToPerform match {
      case (Action.CheckDeepArchiveForOnline, Some(_)) =>
        handleCheckDeepArchiveForOnline(onlineOutputMessage)

      case (Action.CheckInternalArchive, Some(_)) =>
        handleCheckVaultForOnline(internalArchiveVault, onlineOutputMessage, NOT_IMPL_outputInternalArchiveCopyRequired)

      case (Action.CheckExistsOnNearline, Some(_)) =>
        handleCheckVaultForOnline(nearlineVault, onlineOutputMessage, NOT_IMPL_outputNearlineCopyRequired)

      case (Action.ClearAndDelete, Some(_)) =>
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
    val CheckDeepArchiveForNearline, CheckInternalArchive, ClearAndDelete, DropMsg, JustNo, CheckExistsOnNearline, CheckDeepArchiveForOnline  = Value
  }
}
