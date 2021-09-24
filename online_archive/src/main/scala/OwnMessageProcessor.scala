import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.{ArchiveHunterCommunicator, ArchiveHunterConfig}
import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class OwnMessageProcessor(config:ArchiveHunterConfig)
                         (implicit val archivedRecordDAO: ArchivedRecordDAO,
                          ec:ExecutionContext,
                          mat:Materializer,
                          actorSystem: ActorSystem) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  private val archiveHunterCommunicator = new ArchiveHunterCommunicator(config)

  /**
   * records the successful validation to the datbase record and returns an updated copy of said record
   * @param rec the record to update
   * @return a Future, containing either an error string or an updated record.  The Future can fail of a non-recoverable
   *         error occurs, e.g. a database problem or the row in `rec` is not present in the database
   */
  protected def recordSuccessfulValidation(rec: ArchivedRecord) = {
    def checkUpdateCount(rowsUpdated:Int) = {
      if (rowsUpdated == 0 || rec.id.isEmpty) {
        logger.warn(s"No row present in the database for ${rec.id}, maybe it was deleted?")
        Future.failed(new RuntimeException("No row present in the database, maybe it was deleted?"))
      } else {
        if (rowsUpdated > 1) {
          logger.warn(s"Updated $rowsUpdated rows but expected 1, check the database for invalid data")
        }
        Future(rowsUpdated)
      }
    }

    val updateFut = for {
      rowsUpdated <- archivedRecordDAO.updateIdValidationStatus(rec.id.get, true)
      _ <- checkUpdateCount(rowsUpdated)
      maybeUpdatedRecord <- archivedRecordDAO.getRecord(rec.id.get)
    } yield maybeUpdatedRecord

    updateFut.map({
      case Some(updatedRec)=>Right(updatedRec)
      case None=>Left("Could not get updated database record, will retry")
    })
  }

  /**
   * perform validation of the ArchiveHunter ID contained in the message
   *
   * @param msg parsed JSON of the incoming message. This is expected to be an ArchivedRecord and the future will fail
   *            if it can't be unmarshalled.
   * @return a Future, with either a Left indicating a retryable error or a Right with an updated message indicating success.
   *         If a non-retryable error occurs then the Future will be failed.
   */
  def handleArchivehunterValidation(msg: Json) = {
    msg.as[ArchivedRecord] match {
      case Left(err)=>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ArchivedRecord: $err"))
      case Right(rec)=>
        if(rec.archiveHunterIDValidated) {
          logger.info(s"Archivehunter ID ${rec.archiveHunterID} already validated for s3://${rec.uploadedBucket}/${rec.uploadedPath}, leaving it")
          Future(Right(rec))
        } else {
          logger.info(s"Performing archivehunter validation on s3://${rec.uploadedBucket}/${rec.uploadedPath}")
          archiveHunterCommunicator
            .lookupArchivehunterId(rec.archiveHunterID, rec.uploadedBucket, rec.uploadedPath)
            .flatMap({
              case true=> //the ID matches
                logger.info(s"Successfully validated s3://${rec.uploadedBucket}/${rec.uploadedPath}")
                recordSuccessfulValidation(rec)
              case false=> //the ID does not exist
                logger.info(s"Archive hunter ID does not exist, will retry")
                Future(Left("Archivehunter ID does not exist yet, will retry"))
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
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, Json]] = {
    routingKey match {
      case "storagetier.onlinearchive.newfile.success"=>
        handleArchivehunterValidation(msg).map(_.map(_.asJson))
      case _=>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
