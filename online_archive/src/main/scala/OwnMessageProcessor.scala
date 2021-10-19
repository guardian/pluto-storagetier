import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.{ArchiveHunterCommunicator, ArchiveHunterConfig}
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecord, FailureRecordDAO}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import messages.RevalidateArchiveHunterRequest
import org.slf4j.LoggerFactory
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile

import java.nio.file.Paths
import scala.concurrent.{ExecutionContext, Future}

class OwnMessageProcessor(implicit val archivedRecordDAO: ArchivedRecordDAO,
                          failureRecordDAO: FailureRecordDAO,
                          archiveHunterCommunicator: ArchiveHunterCommunicator,
                          vidispineCommunicator:VidispineCommunicator,
                          vidispineFunctions: VidispineFunctions,
                          ec:ExecutionContext,
                          mat:Materializer,
                          actorSystem: ActorSystem) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

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
   * @param rec ArchivedRecord to be validated.
   * @return a Future, with either a Left indicating a retryable error or a Right with an updated message indicating success.
   *         If a non-retryable error occurs then the Future will be failed.
   */
  def handleArchivehunterValidation(rec:ArchivedRecord) = {
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
            logger.info(s"Archive hunter ID for ${rec.originalFilePath} does not exist, will retry")
            Future(Left("Archivehunter ID does not exist yet, will retry"))
        })
    }.recoverWith({
      case err:Throwable=>
        val failure = FailureRecord(
          None,
          rec.originalFilePath,
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

  def handleRevalidationList(msg:Json):Future[Either[String, MessageProcessorReturnValue]] = {
    msg.as[RevalidateArchiveHunterRequest] match {
      case Left(err) =>
        Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into a RevalidateArchiveHunterRequest: $err"))
      case Right(rec) =>
        logger.info(s"Received revalidation request for ${rec.id.length} records: ${rec.id.mkString(",")}")

        val resultFut = for {
          //look up each record by id, and drop the ones that fail
          records <- Future.sequence(rec.id.map(requestedId => archivedRecordDAO.getRecord(requestedId).recover(_ => None)))
          results <- Future.sequence(
            records
              .collect({case Some(rec)=>rec})
              .map(rec=>
                handleArchivehunterValidation(rec)
                  .recover({case err:Throwable=>Left(err.getMessage)})
              )
          )
        } yield results

        resultFut.flatMap(results => {
          val failures = results.collect({ case Left(err) => err })
          if (failures.nonEmpty) {
            logger.warn(s"${failures.length} revalidation requests failed: ")
            failures.foreach(err => logger.warn(s"\t$err"))
          }
          logger.info(s"Revalidation operation complete. Out of ${rec.id.length} requests, ${results.length - failures.length} succeeded")
          //signal we don't need a return value
          Future.failed(SilentDropMessage(Some("No return required")))
        })
    }
  }

  /**
   * for a given item, try to find the thumbnails and proxies and upload them all.
   * this is used when "replaying" existing items in the AssetSweeper database
   * @param vidispineItemId the vidispine item ID to query
   * @param archivedRecord ArchivedRecord corresponding to the original media.
   * @return
   */
  def uploadVidispineBits(vidispineItemId:String, archivedRecord: ArchivedRecord) =
    for {
      itemShapes <- vidispineCommunicator.listItemShapes(vidispineItemId)
      thumbsResult <- vidispineFunctions.uploadThumbnailsIfRequired(vidispineItemId, None, archivedRecord)
      shapesResult <- itemShapes match {
        case Some(shapeDocs)=>
          logger.info(s"Found ${shapeDocs.length} shapes for $vidispineItemId: ${shapeDocs.map(_.summaryString)}")
          Future.sequence(
            shapeDocs.map(shape=>{
              vidispineFunctions
                .uploadShapeIfRequired(vidispineItemId,shape.id, shape.tag.headOption.getOrElse(""), archivedRecord)
                .recover({
                  case err:SilentDropMessage=>  //don't allow SilentDropMessage to break our loop here
                    Left("ignored")
                })
            })
          )
        case None=>Future(Seq())
      }
      _ <- vidispineFunctions.uploadMetadataToS3(vidispineItemId, None, archivedRecord)
    } yield shapesResult

  private def handleReplayUpload(newFile:AssetSweeperNewFile, vidispineItemId:String, filePathString:String, archivedRecord:ArchivedRecord) =
    for {
      results <- uploadVidispineBits(vidispineItemId, archivedRecord)
      finalResult <- Future {
        val failures = results.collect({case Left(err)=>err}).filter(msg=>msg!="ignored")
        if(failures.nonEmpty) {
          logger.warn(s"Could not upload ${failures.length} assets for ${newFile.imported_id}: ")
          failures.foreach(msg=>logger.warn(s"\t$msg\n"))
          if(failures.head=="no archivedRecord") {
            Left(s"no ArchivedRecord found for $filePathString")
          } else {
            Left(s"${failures.length} assets failed upload")
          }
        } else {
          val successes = results.collect({case Right(json)=>json})
          if(successes.isEmpty) {
            logger.info(s"No extra assets to upload for ${newFile.imported_id}")
            throw SilentDropMessage()
          } else {
            logger.info(s"Successfully uploaded ${results.length} assets for ${newFile.imported_id}")
            Right(successes.head)
          }
        }
      }
    } yield finalResult

  def handleReplayStageTwo(msg:Json) = {
    msg.as[AssetSweeperNewFile] match {
      case Right(newFile) =>
        newFile.imported_id match {
          case Some(vidispineItemId) =>
            val filePathString = Paths.get(newFile.filepath, newFile.filename).toString
            val validationResult = for {
              maybeArchivedRecord <- archivedRecordDAO.findBySourceFilename(filePathString)
              maybeArchiveHunterValidated <- maybeArchivedRecord match {
                case Some(archivedRecord)=>handleArchivehunterValidation(archivedRecord)
                case None=>
                  Future.failed(new RuntimeException(s"The given asset sweeper record for ${newFile.filepath}/${newFile.filename} is not imported yet"))
              }
            } yield maybeArchiveHunterValidated

            validationResult.flatMap({
              case Left(retryableErr)=>
                Future(Left(retryableErr))
              case Right(updatedArchivedRecord)=>
                handleReplayUpload(newFile, vidispineItemId, filePathString, updatedArchivedRecord)
            })
          case None =>
            logger.info(s"The item at ${newFile.filepath}/${newFile.filename} is not ingested to Vidispine yet.")
            Future.failed(SilentDropMessage())
        }
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
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
    routingKey match {
      case "storagetier.onlinearchive.newfile.success"=>
        msg.as[ArchivedRecord] match {
          case Left(err)=>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ArchivedRecord: $err"))
          case Right(file)=>
            handleArchivehunterValidation(file)
              .map(_.map(_.asJson))
        }

      case "storagetier.onlinearchive.replay"=>
        handleReplayStageTwo(msg)
      case "storagetier.onlinearchive.request.archivehunter-revalidation"=>
        handleRevalidationList(msg)
      case _=>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
