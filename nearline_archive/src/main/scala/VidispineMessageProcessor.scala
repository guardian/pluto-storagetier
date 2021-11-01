import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
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
import matrixstore.MatrixStoreConfig

import java.nio.file.{Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure}

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
                Right(MessageProcessorReturnValue(
                  record
                    .copy(id=Some(recId))
                    .asJson
                ))
              )

          case Left(error) => Future(Left(error))
        })
    }).recoverWith(err => {
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
      case (_, _)=>
        logger.warn(s"Dropping message $routingKey from vidispine exchange as I don't know how to handle it. This should be fixed in" +
          s" the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
