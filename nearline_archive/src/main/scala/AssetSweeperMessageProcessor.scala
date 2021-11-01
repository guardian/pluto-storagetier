import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecord, FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.slf4j.{LoggerFactory}

import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future}

class AssetSweeperMessageProcessor()
                                  (implicit nearlineRecordDAO: NearlineRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   ec:ExecutionContext,
                                   mat:Materializer,
                                   system:ActorSystem,
                                   matrixStoreBuilder: MXSConnectionBuilder,
                                   mxsConfig: MatrixStoreConfig,
                                   fileCopier: FileCopier) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)
  import AssetSweeperNewFile.Decoder._

  def copyFile(vault: Vault, file: AssetSweeperNewFile, maybeNearlineRecord: Option[NearlineRecord]): Future[Either[String, Json]] = {
    val fullPath = Paths.get(file.filepath, file.filename)
    val maybeObjectId = maybeNearlineRecord.map(rec => rec.objectId)

    fileCopier.copyFileToMatrixStore(vault, file.filename, fullPath, maybeObjectId)
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
                  .copy(id=Some(recId), originalFilePath = fullPath.toString)
                  .asJson
              )
            )
        case Left(error) => Future(Left(error))
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

  def processFile(file: AssetSweeperNewFile, vault: Vault): Future[Either[String, Json]] = {
    val fullPath = Paths.get(file.filepath, file.filename)
    nearlineRecordDAO
      .findBySourceFilename(fullPath.toString)
      .flatMap(rec => {
        copyFile(vault, file, rec)
      })
  }

  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    if(!routingKey.endsWith("new") && !routingKey.endsWith("update")) return Future.failed(SilentDropMessage())

    msg.as[AssetSweeperNewFile] match {
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
      case Right(file)=>
        if (routingKey=="assetsweeper.asset_folder_importer.file.update") {
          logger.warn("Received an update message, these are not implemented yet")
          Future(Left("Not implemented yet"))
        } else {
          val fullPath = Paths.get(file.filepath, file.filename)
          if(!Files.exists(fullPath) || !Files.isRegularFile(fullPath)) {
            logger.error(s"File $fullPath does not exist, or it's not a regular file. Can't continue.")
            Future.failed(new RuntimeException("Invalid file"))
          } else {
            matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
              processFile(file, vault)
            }
          }
        }
    }
  }
}
