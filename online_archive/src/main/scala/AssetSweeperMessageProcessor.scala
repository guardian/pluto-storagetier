import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, ErrorComponents, FailureRecord, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO, RetryStates}
import io.circe.Json
import messages.AssetSweeperNewFile
import io.circe.generic.auto._
import plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import io.circe.syntax._
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class AssetSweeperMessageProcessor(plutoCoreConfig:PlutoCoreConfig)
                                  (implicit archivedRecordDAO: ArchivedRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   ignoredRecordDAO: IgnoredRecordDAO,
                                   ec:ExecutionContext,
                                   mat:Materializer,
                                   system:ActorSystem,
                                   uploader: FileUploader) extends MessageProcessor {
  private lazy val asLookup = new AssetFolderLookup(plutoCoreConfig)
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

  def processFileAndProject(file:AssetSweeperNewFile, fullPath:Path, maybeProject: Option[ProjectRecord]):Future[Either[String, Json]] = {
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
        val relativePath = asLookup.relativizeFilePath(fullPath).toString
        logger.info(s"Archiving file '$fullPath' to s3://${uploader.bucketName}/$relativePath")
        Future.fromTry(uploader.copyFileToS3(fullPath.toFile, Some(relativePath))).flatMap(fileName=>{
          logger.debug(s"$fullPath: Upload completed")
          val archiveHunterID = utils.ArchiveHunter.makeDocId(bucket = uploader.bucketName, fileName)
          logger.debug(s"archivehunter ID for $relativePath is $archiveHunterID")
            val rec = ArchivedRecord(archiveHunterID,
              originalFilePath=relativePath,
              uploadedBucket = uploader.bucketName,
              uploadedPath = fileName,
              uploadedVersion = None)

            archivedRecordDAO.writeRecord(rec).map(recId=>Right(rec.copy(id=Some(recId)).asJson))
        }).recoverWith(err=>{
          val rec = FailureRecord(id = None,
            originalFilePath = relativePath,
            attempt = 1,  //FIXME: need to be passed the retry number by the Framework
            errorMessage = err.getMessage,
            errorComponent = ErrorComponents.Internal,
            retryState = RetryStates.WillRetry)
          failureRecordDAO.writeRecord(rec).map(_=>Left(err.getMessage))
        })
      case Some(reason)=>
        val rec = IgnoredRecord(None, fullPath.toString, reason, None, None)
        //record the fact we ignored the file to the database. This should not raise duplicate record errors.
        ignoredRecordDAO
          .writeRecord(rec)
          .map(writtenRecord=> {
            Right(writtenRecord.asJson)
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
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, Json]] = {
    msg.as[AssetSweeperNewFile] match {
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
      case Right(newFile)=>
        if(routingKey=="assetsweeper.asset_folder_importer.file.update") {
          logger.warn("Received an update message, these are not implemented yet")
          Future(Left("not implemented yet"))
        } else {
          for {
            fullPath <- compositingGetPath(newFile)
            projectRecord <- asLookup.assetFolderProjectLookup(fullPath)
            result <- processFileAndProject(newFile, fullPath, projectRecord)
          } yield result
        }
    }
  }
}
