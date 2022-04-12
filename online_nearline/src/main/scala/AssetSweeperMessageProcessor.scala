import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future}

class AssetSweeperMessageProcessor()
                                  (implicit nearlineRecordDAO: NearlineRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   ec:ExecutionContext,
                                   mat:Materializer,
                                   system:ActorSystem,
                                   matrixStoreBuilder: MXSConnectionBuilderImpl,
                                   mxsConfig: MatrixStoreConfig,
                                   vidispineCommunicator: VidispineCommunicator,
                                   fileCopier: FileCopier) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)
  import AssetSweeperNewFile.Codec._

  def copyFile(vault: Vault, file: AssetSweeperNewFile, maybeNearlineRecord: Option[NearlineRecord]): Future[Either[String, Json]] = {
    val fullPath = Paths.get(file.filepath, file.filename)

    fileCopier.copyFileToMatrixStore(vault, file.filename, fullPath)
      .flatMap({
        case Right(objectId) =>
          val record = maybeNearlineRecord match {
            case Some(rec) => rec
            case None => NearlineRecord(objectId, fullPath.toString)
          }

          nearlineRecordDAO
            .writeRecord(record)
            .map(recId=>
              Right(
                record
                  .copy(id=Some(recId), originalFilePath = fullPath.toString, expectingVidispineId = !file.ignore)
                  .asJson
              )
            )
        case Left(error) => Future(Left(error))
      }).recoverWith({
        case err:BailOutException=>
          logger.warn(s"A permanent exception occurred when trying to copy $fullPath: ${err.getMessage}")
          Future.failed(err)
        case err:Throwable=>
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
   * performs a search on the given vault looking for a matching file (i.e. one with the same file name AND size).
   * If one exists, it will create a NearlineRecord linking that file to the given name, save it to the database and return it.
   * Intended for use if no record exists already but a file may exist on the storage.
   * @param vault vault to check
   * @param file AssetSweeperNewFile record
   * @return a Future, containing the newly saved NearlineRecord if a record is found or None if not.
   */
  protected def checkForPreExistingFiles(vault:Vault, file:AssetSweeperNewFile) = {
    val filePath = Paths.get(file.filepath, file.filename)

    for {
      matchingNearlineFiles <- fileCopier.findMatchingFilesOnNearline(vault, filePath, file.size)
      maybeVidispineMatches <- vidispineCommunicator.searchByPath(filePath.toString).recover({
        case err:Throwable=>  //don't abort the process if VS is not playing nice
          logger.error(s"Could not consult Vidispine for new file $filePath: ${err.getMessage}")
          None
      })  //check if we have something in Vidispine too
      result <- if(matchingNearlineFiles.isEmpty) {
        logger.info(s"Found no pre-existing archived files for $filePath")

        Future(None)
      } else {
        logger.info(s"Found ${matchingNearlineFiles.length} archived files for $filePath: ${matchingNearlineFiles.map(_.pathOrFilename).mkString(",")}")
        val newRec = NearlineRecord(
          objectId=matchingNearlineFiles.head.oid,
          originalFilePath = filePath.toString,
        )

        val updatedRec = maybeVidispineMatches.flatMap(_.file.headOption).flatMap(_.item.map(_.id)) match {
          case Some(vidispineId)=>newRec.copy(vidispineItemId = Some(vidispineId))
          case None=>newRec
        }
        nearlineRecordDAO.writeRecord(updatedRec).map(newId=>Some(updatedRec.copy(id=Some(newId))))
      }
    } yield result
  }

  def processFile(file: AssetSweeperNewFile, vault: Vault): Future[Either[String, Json]] = {
    val fullPath = Paths.get(file.filepath, file.filename)

    for {
      maybeNearlineRecord <- nearlineRecordDAO.findBySourceFilename(fullPath.toString)  //check if we have a record of this file in the database
      maybePreExistingRecord   <- if(maybeNearlineRecord.isEmpty) checkForPreExistingFiles(vault, file) else Future(None) //if not then check the appliance itself
      result              <- if(maybePreExistingRecord.isDefined) Future(Right(maybePreExistingRecord.asJson)) else copyFile(vault, file, maybeNearlineRecord)
    } yield result
  }

  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    if(!routingKey.endsWith("new") && !routingKey.endsWith("update")) return Future.failed(SilentDropMessage())

    msg.as[AssetSweeperNewFile] match {
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
      case Right(file)=>
        val fullPath = Paths.get(file.filepath, file.filename)
        if(!Files.exists(fullPath) || !Files.isRegularFile(fullPath)) {
          logger.error(s"File $fullPath does not exist, or it's not a regular file. Can't continue.")
          Future.failed(SilentDropMessage(Some(s"Invalid file $fullPath")))
        } else {
          matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
            processFile(file, vault)
          }
        }
      }
  }
}
