import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecord, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO}
import io.circe.Json
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import io.circe.generic.auto._
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import io.circe.syntax._
import org.slf4j.{LoggerFactory, MDC}
import utils.ArchiveHunter
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._

class AssetSweeperMessageProcessor(plutoCoreConfig:PlutoCoreConfig)
                                  (implicit archivedRecordDAO: ArchivedRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   ignoredRecordDAO: IgnoredRecordDAO,
                                   vidispineFunctions: VidispineFunctions,
                                   vidispineCommunicator: VidispineCommunicator,
                                   ec:ExecutionContext,
                                   mat:Materializer,
                                   system:ActorSystem,
                                   uploader: FileUploader) extends MessageProcessor {
  protected lazy val asLookup = new AssetFolderLookup(plutoCoreConfig)
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * assembles a java.nio.Path pointing to the Sweeper file, catching exceptions and converting to a Future
   * to make it easier to compose
   * @param newFile
   * @return
   */
  private def compositingGetPath(newFile:AssetSweeperNewFile) = Future.fromTry(
    Try {
      Paths.get(newFile.filepath, newFile.filename)
    })

  private def callUpload(fullPath:Path, relativePath:Path) = {
    logger.info(s"Archiving file '$fullPath' to s3://${uploader.bucketName}/$relativePath")
    Future.fromTry(
      uploader.copyFileToS3(fullPath.toFile, Some(relativePath.toString))
    ).flatMap((fileInfo)=>{
      val (fileName, fileSize) = fileInfo
      logger.debug(s"$fullPath: Upload completed")
      val archiveHunterID = ArchiveHunter.makeDocId(bucket = uploader.bucketName, fileName)
      logger.debug(s"archivehunter ID for $relativePath is $archiveHunterID")
      archivedRecordDAO
        .findBySourceFilename(fullPath.toString)
        .map({
          case Some(existingRecord)=>
            existingRecord.copy(
              uploadedBucket = uploader.bucketName,
              uploadedPath = fileName,
              uploadedVersion = None
            )
          case None=>
            ArchivedRecord(archiveHunterID,
              originalFilePath=fullPath.toString,
              originalFileSize=fileSize,
              uploadedBucket = uploader.bucketName,
              uploadedPath = fileName,
              uploadedVersion = None)
        }).flatMap(rec=>{
          archivedRecordDAO
            .writeRecord(rec)
            .map(recId =>
              Right(
                rec
                  .copy(id = Some(recId))
                  .asJson
              )
          )
      })
    }).recoverWith(err=>{
      logger.error(s"Could not complete upload for $fullPath ${err.getMessage}", err)
      val attemptCount = attemptCountFromMDC() match {
        case Some(count)=>count
        case None=>
          logger.warn(s"Could not get attempt count from logging context for ${fullPath.toString}, creating failure report with attempt 1")
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

  def processFileAndProject(fullPath:Path, maybeProject: Option[ProjectRecord]):Future[Either[String, MessageProcessorReturnValue]] = {
    val ignoreReason = maybeProject match {
      case Some(project)=>
        if(project.deletable.getOrElse(false)) {  //If the project is marked as “deletable”, record to datastore as “ignored”
          logger.info(s"Not archiving '$fullPath' as it belongs to '${project.title}' (${project.id.map(i=>s"project id $i").getOrElse("no project id")}) which is marked as deletable")
          Some(s"project ${project.id.getOrElse(-1)} is deletable")
        } else if(project.sensitive.getOrElse(false)) {
          logger.info(s"Not archiving '$fullPath' as it belongs to '${project.title}' (${project.id.map(i=>s"project id $i").getOrElse("no project id")}) which is marked as sensitive")
          Some(s"project ${project.id.getOrElse(-1)} is sensitive")
        } else {
          None
        }
      case None=>
        logger.warn(s"No project could be found that is associated with $fullPath, assuming that it does need external archive")
        None
    }

    ignoreReason match {
      case None=> //no reason to ignore - we should archive
        asLookup.relativizeFilePath(fullPath) match {
          case Left(err)=>
            logger.error(s"Could not relativize file path $fullPath: $err. Uploading to $fullPath")
            callUpload(fullPath, fullPath)
          case Right(relativePath)=>
            callUpload(fullPath, relativePath)
        }

      case Some(reason)=>
        val rec = IgnoredRecord(None, fullPath.toString, reason, None, None)
        //record the fact we ignored the file to the database. This should not raise duplicate record errors.
        ignoredRecordDAO
          .writeRecord(rec)
          .map(recId=> {
            Right(rec.copy(id=Some(recId)).asJson)
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
    if(routingKey.startsWith("assetsweeper.asset_folder_importer.file")) {
      handleNewFile(routingKey, msg)
    } else if(routingKey=="assetsweeper.replay.file") {
      handleReplay(routingKey, msg)
    } else {
      Future.failed(new RuntimeException(s"Did not recognise routing key $routingKey"))
    }
  }

  /**
   * handle a replay notification. This is telling us that we should check that the given record is correctly handled on
   * our side, as it may have been missed before
   * @param routingKey
   * @param msg
   * @return
   */
  def handleReplay(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    import AssetSweeperNewFile.Decoder._  //need to use custom decoder to properly decode message
    msg.as[AssetSweeperNewFile] match {
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
      case Right(newFile)=>
        for {
          fullPath <- compositingGetPath(newFile)
          projectRecord <- asLookup.assetFolderProjectLookup(fullPath)
          fileUploadResult <- processFileAndProject(fullPath, projectRecord)
        } yield fileUploadResult
    }
  }

  /**
   * handle a notification from Asset Sweeper that a file has been found or updated
   * @param routingKey message routing key
   * @param msg message parsed content
   * @return
   */
  def handleNewFile(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    import AssetSweeperNewFile.Decoder._  //need to use custom decoder to properly decode message
    if(!routingKey.endsWith("new") && !routingKey.endsWith("update")) return Future.failed(SilentDropMessage())
    msg.as[AssetSweeperNewFile] match {
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
      case Right(newFile)=>
        if (routingKey=="assetsweeper.asset_folder_importer.file.update") {
          logger.warn("Received an update message, these are not implemented yet")
          Future(Left("not implemented yet"))
        } else {
          for {
            fullPath <- compositingGetPath(newFile)
            projectRecord <- asLookup.assetFolderProjectLookup(fullPath)
            result <- processFileAndProject(fullPath, projectRecord)
          } yield result
        }.recoverWith({
          case err:Throwable=>
            val failure = FailureRecord(
              None,
              Paths.get(newFile.filepath, newFile.filename).toString,
              1,
              s"Uncaught exception: ${err.getMessage}",
              ErrorComponents.Internal,
              RetryStates.RanOutOfRetries
            )
            failureRecordDAO
              .writeRecord(failure)
              .flatMap(_=>Future.failed(err))
        })
    }
  }
}
