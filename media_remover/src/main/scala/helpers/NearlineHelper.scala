package helpers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.helpers.{MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.storagetier.framework.{MessageProcessorConverters, MessageProcessorReturnValue, RMQDestination}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.PendingDeletionRecordDAO
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecordDAO
import com.gu.multimedia.storagetier.plutocore.AssetFolderLookup
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import exceptions.BailOutExceptionMR
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class NearlineHelper(asLookup: AssetFolderLookup) (
  implicit
  pendingDeletionRecordDAO: PendingDeletionRecordDAO,
  nearlineRecordDAO: NearlineRecordDAO,
  ec: ExecutionContext,
  mat: Materializer,
  system: ActorSystem,
  matrixStoreBuilder: MXSConnectionBuilderImpl,
  mxsConfig: MatrixStoreConfig,
  vidispineCommunicator: VidispineCommunicator,
//  s3ObjectChecker: S3ObjectChecker,
  checksumChecker: ChecksumChecker,
  pendingDeletionHelper: PendingDeletionHelper
  )  {

    private val logger = LoggerFactory.getLogger(getClass)

  /**
   * If the given file already exists in the MXS vault (i.e., there is a file with a matching MXFS_PATH _and_ checksum
   * _and_ file size, then a Future(true) is returned
   * If there is a file with matching MXFS_PATH but checksum and/or size do not match, then a Future(false) is returned
   * We demand that the `maybeLocalChecksum` actually has a value, otherwise we will return Future(false) and log a
   * warning
   *
   * @param vault Vault to check
   *                               //   * @param fileName file name to check on the Vault
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
   * Searches the given vault for files matching the given specification.
   * Both the name and fileSize must match in order to be considered valid.
   *
   * @param vault    vault to search
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
  def verifyChecksumMatchUsingChecker(filePath: String, potentialFiles: Seq[MxsObject], maybeLocalChecksum: Option[String]): Future[Option[String]] = {
    val verifiedMaybeLocalChecksum = maybeLocalChecksum match {
      case None => throw new RuntimeException("Must be called with Some(actualChecksum)")
      case Some(value) => Some(value)
    }
    checksumChecker.verifyChecksumMatch(Paths.get(filePath), potentialFiles, verifiedMaybeLocalChecksum)
  }

  protected def openMxsObject(vault:Vault, oid:String): Try[MxsObject] = Try { vault.getObject(oid) }
  protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = MatrixStoreHelper.findByFilenameNew(vault, fileName)
  protected def callObjectMatrixEntryFromOID(vault: Vault, fileName: String): Future[ObjectMatrixEntry] = ObjectMatrixEntry.fromOID(fileName, vault)

  def getNearlineFileSize(vault: Vault, oid: String): Future[Long] =
    Future.fromTry(Try {vault.getObject(oid)}.map(MetadataHelper.getFileSize))

  def getChecksumForNearline(vault: Vault, oid: String): Future[Option[String]] =
    for {
      mxsFile <- Future.fromTry(openMxsObject(vault, oid))
      maybeMd5 <- MatrixStoreHelper.getOMFileMd5(mxsFile).flatMap({
        case Failure(err) =>
          logger.error(s"Unable to get checksum from appliance ${vault.getId}, file with $oid should be considered unsafe", err)
          Future(None)
        case Success(remoteChecksum) =>
          logger.info(s"Appliance reported checksum of $remoteChecksum for $oid on ${vault.getId}")
          Future(Some(remoteChecksum))
      })
    } yield maybeMd5

  def nearlineExistsInInternalArchive(vault: Vault, internalArchiveVault: Vault, nearlineId: String, filePath: String, fileSize: Long): Future[Boolean] = {
    val filePathBack = putItBack(filePath)
    for {
      maybeChecksum <- getChecksumForNearline(vault, nearlineId)
      //TODO add nearlineId to parameter list for logging purposes(?)
      exists <- existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, internalArchiveVault, filePathBack, filePathBack, fileSize, maybeChecksum)
    } yield exists
  }

  def putItBack(filePath: String): String = filePath match {
    case f if f.startsWith("/") =>
      logger.debug(s"$filePath is absolute, returning without putting base back")
      filePath
    case _ => asLookup.putBackBase(Paths.get(filePath)) match {
      case Left(err) =>
        logger.warn(s"Could not but back base path for $filePath: $err. Will use as was.")
        filePath
      case Right(value) =>
        logger.debug(s"$filePath had the base put back to form ${value.toString}")
        value.toString
    }
  }

  def deleteMediaFromNearline(vault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]): Future[Either[String, MessageProcessorReturnValue]] =
    (mediaTier, filePathMaybe, nearlineIdMaybe) match {
      case ("NEARLINE", Some(filepath), Some(nearlineId)) =>
        // Wait for ATT files to be handled before we delete the main object, since we need the metadata of the main object
        dealWithAttFiles(vault, nearlineId, filepath).flatMap({
          case Left(err) =>
            logger.warn(err)
            MessageProcessorConverters.futureEitherToMPRV(deleteMainNearlineMedia(vault, nearlineId, filepath, vidispineItemIdMaybe))
          case Right(value) =>
            logger.debug(value)
            MessageProcessorConverters.futureEitherToMPRV(deleteMainNearlineMedia(vault, nearlineId, filepath, vidispineItemIdMaybe))
        })
      case (_, _, _) => throw new RuntimeException(s"Cannot delete from nearline, wrong media tier ($mediaTier), or missing nearline id ($nearlineIdMaybe)")
    }


  private def deleteMainNearlineMedia(vault: Vault, nearlineId: String, filepath: String, vidispineItemIdMaybe: Option[String]): Future[Either[String, Json]] =
    // TODO do we need to wrap this with a Future.fromTry?
    Try {
      vault.getObject(nearlineId).delete()
    } match {
      case Success(_) =>
        logger.info(s"Nearline media oid=$nearlineId, path=$filepath removed")
        Future(Right(MediaRemovedMessage(mediaTier = MediaTiers.NEARLINE.toString, originalFilePath = filepath, nearlineId = Some(nearlineId), vidispineItemId = vidispineItemIdMaybe).asJson))
      case Failure(exception) =>
        logger.warn(s"Failed to remove nearline media oid=$nearlineId, path=$filepath, reason: ${exception.getMessage}")
        Future(Left(s"Failed to remove nearline media oid=$nearlineId, path=$filepath, reason: ${exception.getMessage}"))
    }

  def dealWithAttFiles(vault: Vault, nearlineId: String, originalFilePath: String): Future[Either[String, String]] = {
    val combinedRes = (for {
      objectMatrixEntry <- callObjectMatrixEntryFromOID(vault, nearlineId)
      proxyRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_PROXY_OID", nearlineId, originalFilePath)
      thumbRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_THUMB_OID", nearlineId, originalFilePath)
      metaRes <- deleteSingleAttMedia(vault, objectMatrixEntry, "ATT_META_OID", nearlineId, originalFilePath)
    } yield (proxyRes, thumbRes, metaRes))
      .recover({
        case err: Throwable =>
          logger.warn(s"Something went wrong while trying to get metadata for $nearlineId: $err")
          Left(s"Something went wrong while trying to get metadata to delete ATT files for $nearlineId: $err")
      })

    combinedRes.map {
      case (Right(_), Right(_), Right(_)) => Right(s"The ATT files that were present for $nearlineId have been deleted")
      case (_, _, _) => Left(s"One or more ATT files for $nearlineId could not be deleted. Please look at logs for more info")
      case Left(err: String) => Left(err)
    }
  }

  def deleteSingleAttMedia(vault: Vault, objectMatrixEntry: ObjectMatrixEntry, attributeKey: String, nearlineIdForLog: String, filePathForLog: String): Future[Either[String, Boolean]] =
    objectMatrixEntry.stringAttribute(attributeKey) match {
      case None =>
        logger.info(s"No $attributeKey to remove for main nearline media oid=$nearlineIdForLog, path=$filePathForLog")
        Future(Right(false))
      case Some(attOid) =>
        Try {
          vault.getObject(attOid).delete()
        } match {
          case Success(_) =>
            logger.info(s"$attributeKey with oid $attOid removed for main nearline media oid=$nearlineIdForLog, path=$filePathForLog")
            Future(Right(true))
          case Failure(exception) =>
            logger.warn(s"Failed to remove nearline media oid=$nearlineIdForLog, path=$filePathForLog, reason: ${exception.getMessage}")
            Future(Left(s"Failed to remove $attributeKey with oid=$attOid for main nearline media oid=$nearlineIdForLog, path=$filePathForLog, reason: ${exception.getMessage}"))
        }
    }

  // online IA
  def outputInternalArchiveCopyRequiredForOnline(vidispineItemId: String, originalFilePath: String): Future[Either[String, MessageProcessorReturnValue]] =
    nearlineRecordDAO.findByVidispineId(vidispineItemId).map {
      case Some(rec) => Right(MessageProcessorReturnValue(rec.asJson, Seq(RMQDestination("storagetier-media-remover", "storagetier.nearline.internalarchive.required.online"))))
      case None => throw new RuntimeException(s"Cannot request internalArchiveCopy, no record found for ${vidispineItemId} ${originalFilePath}")
    }

  // nearline IA
  def outputInternalArchiveCopyRequiredForNearline(oid: String, originalFilePathMaybe: Option[String]): Future[Either[String, MessageProcessorReturnValue]] =
    nearlineRecordDAO.findBySourceFilename(originalFilePathMaybe.get).map {
      case Some(rec) => Right(MessageProcessorReturnValue(rec.asJson, Seq(RMQDestination("storagetier-media-remover", "storagetier.nearline.internalarchive.required.nearline"))))
      case None => throw new RuntimeException(s"Cannot request internalArchiveCopy, no record found for oid ${oid} ${originalFilePathMaybe}")
    }

}
