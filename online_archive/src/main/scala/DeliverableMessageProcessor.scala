import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, ErrorComponents, FailureRecord, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO, RetryStates}
import io.circe.Json
import messages.DeliverableAssetMessage
import io.circe.generic.auto._
import org.slf4j.LoggerFactory
import plutodeliverables.PlutoDeliverablesConfig
import utils.ArchiveHunter
import io.circe.syntax._

import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DeliverableMessageProcessor(config:PlutoDeliverablesConfig, uploader:FileUploader, uploadBucket:String)(implicit val archivedRecordDAO:ArchivedRecordDAO, failureRecordDAO: FailureRecordDAO, ignoredRecordDAO: IgnoredRecordDAO, ec:ExecutionContext) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def validatePathName(from:Option[String]) = from match {
    case None=>
      Future.failed(new RuntimeException("Message did not provide a path to work with"))
    case Some(pathString)=>
      val p = Paths.get(pathString)
      if(Files.exists(p)) {
        Future(p)
      } else {
        Future.failed(new RuntimeException(s"The indicated file ${p.toString} does not exist"))
      }
  }

  protected def makeUploadPath(sourcePath:Path) = {
    Paths.get(config.uploadBasePath).resolve(
      sourcePath.subpath(sourcePath.getNameCount-config.presevePathParts,sourcePath.getNameCount)
    )
  }

  protected def findExistingRecord(forPath:Path) = archivedRecordDAO.findBySourceFilename(forPath.toAbsolutePath.toString)

  protected def ensureItsUploaded(pathName:Path, maybeExistingRecord:Option[ArchivedRecord]) = uploader.copyFileToS3(pathName.toFile, Some(makeUploadPath(pathName).toString)) match {
    case Success(uploadedFile)=>
      logger.info(s"Successfully ensured that ${pathName.toString} exists in archive at ${uploadedFile._1}")
      val possibleArchiveHunterId = ArchiveHunter.makeDocId(uploadBucket, uploadedFile._1)
      val recordToWrite = maybeExistingRecord match {
        case None=>
          ArchivedRecord(possibleArchiveHunterId, pathName.toString, uploadedFile._2, uploadBucket, uploadedFile._1, None)
        case Some(rec)=>
          rec.copy(
            uploadedBucket = uploadBucket,
            uploadedPath = uploadedFile._1,
            //FIXME: should put a version in here
          )
      }
      archivedRecordDAO
        .writeRecord(recordToWrite)
        .map(recId=>Right(recordToWrite.copy(id=Some(recId)).asJson))
    case Failure(err)=>
      logger.error(s"Could not check for upload of ${pathName.toString}: ${err.getMessage}", err)
      //FIXME: we can't see the attempt number here! Needs to be passed through from the framework
      val recordToWrite = FailureRecord(None, pathName.toString, 1, err.getMessage, ErrorComponents.AWS, RetryStates.WillRetry)
      failureRecordDAO
        .writeRecord(recordToWrite)
        .map(_=>Left(err.getMessage))
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
    msg.as[DeliverableAssetMessage] match {
      case Left(err)=>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ArchivedRecord: $err"))
      case Right(msg)=>
        logger.info(s"Received $routingKey for ${msg.absolute_path}")

        val preparedData = for {
          pathName <- validatePathName(msg.absolute_path)
          maybeExistingRecord <- findExistingRecord(pathName)
        } yield (pathName, maybeExistingRecord)

        preparedData.flatMap({
          case (pathName, maybeExistingRecord)=>
            ensureItsUploaded(pathName, maybeExistingRecord)
          case _=>
            Future.failed(new RuntimeException("Internal error in DeliverableMessageProcessor.handleMessage, prepared data was not provided"))
        })
    }
  }
}
