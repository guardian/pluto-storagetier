import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.helpers.{Copier, MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.streamcomponents.ChecksumSink
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
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Paths
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AssetSweeperMessageProcessor()
                                  (implicit nearlineRecordDAO: NearlineRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   ec:ExecutionContext,
                                   mat:Materializer,
                                   system:ActorSystem,
                                   matrixStoreBuilder: MXSConnectionBuilder,
                                   mxsConfig: MatrixStoreConfig) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)
  import AssetSweeperNewFile.Decoder._

  protected def copyUsingHelper(vault: Vault, file: AssetSweeperNewFile) = {
    val fromFile = Paths.get(file.filepath, file.filename).toFile

    Copier.doCopyTo(vault, Some(file.filename), fromFile, 2*1024*1024, "md5")
  }

  def copyFile(vault: Vault, file: AssetSweeperNewFile, maybeNearlineRecord: Option[NearlineRecord]): Future[Either[String, Json]] = {
    copyUsingHelper(vault, file)
      .flatMap((result) => {
        val (objectId, _) = result

        val record = maybeNearlineRecord match {
          case Some(rec) => rec
          case None => NearlineRecord(objectId, file.filepath)
        }

        nearlineRecordDAO
          .writeRecord(record)
          .map(recId=>
            Right(
              record
                .copy(id=Some(recId), originalFilePath = file.filepath)
                .asJson
            )
          )
      }).recoverWith(err=>{
        val attemptCount = attemptCountFromMDC() match {
          case Some(count)=>count
          case None=>
            logger.warn(s"Could not get attempt count from logging context for ${file.filepath}, creating failure report with attempt 1")
            1
        }

        val rec = FailureRecord(id = None,
          originalFilePath = file.filepath,
          attempt = attemptCount,
          errorMessage = err.getMessage,
          errorComponent = ErrorComponents.Internal,
          retryState = RetryStates.WillRetry)
        failureRecordDAO.writeRecord(rec).map(_=>Left(err.getMessage))
    })
  }

  def processFile(file: AssetSweeperNewFile, vault: Vault): Future[Either[String, Json]] =
    nearlineRecordDAO
      .findBySourceFilename(file.filepath)
      .flatMap({
        case Some(rec) =>
          val fileChecksum =  FileIO.fromPath(Paths.get(file.filepath)).runWith(ChecksumSink.apply)

          Try { vault.getObject(rec.objectId) } match {
            case Success(msxFile) =>
              // File exist in ObjectMatrix check size and md5
              val metadata = MetadataHelper.getMxfsMetadata(msxFile)
              val checksum = MatrixStoreHelper.getOMFileMd5(msxFile)

              if (metadata.size() == file.size && checksum == fileChecksum) {
                logger.info(s"Object with object id ${rec.objectId} and filepath ${file.filepath} already exists")
                Future(Right(rec.asJson))
              }

              copyFile(vault, file, Some(rec))
            case Failure(err) =>
              err.getMessage match {
                case "java.io.IOException: Invalid object, it does not exist (error 306)" =>
                  copyFile(vault, file, Some(rec))
                case _ =>
                  // Error contacting ObjectMatrix
                  logger.info(s"Failed to get object from vault ${file.filepath}, will retry")
                  Future(Left(s"Failed to get object from vault ${file.filepath}, will retry"))
              }
          }
        case None =>
          copyFile(vault, file, None)
      })

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
          matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
            processFile(file, vault)
          }
        }
    }
  }
}
