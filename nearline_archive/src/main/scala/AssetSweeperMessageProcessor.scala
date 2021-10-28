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
import org.slf4j.{LoggerFactory, MDC}

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
    val fullPath = Paths.get(file.filepath, file.filename)
    copyUsingHelper(vault, file)
      .flatMap((result) => {
        val (objectId, _) = result

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
      }).recoverWith(err=>{
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
      .flatMap({
        case None =>                  //no record exists in the nearline table yet => we have not processed this file. Proceed to copy.
          copyFile(vault, file, None)
        case Some(rec) =>             //a record already exists in the nearline table - check if the file state is ok on destination before copying
          Future.fromTry(Try { vault.getObject(rec.objectId) })
            .flatMap({ msxFile =>
              // File exist in ObjectMatrix check size and md5
              val metadata = MetadataHelper.getMxfsMetadata(msxFile)

              val savedContext = MDC.getCopyOfContextMap  //need to save the debug context for when we go in and out of akka
              val checksumMatchFut = Future.sequence(Seq(
                FileIO.fromPath(fullPath).runWith(ChecksumSink.apply),
                MatrixStoreHelper.getOMFileMd5(msxFile)
              )).map(results=>{
                MDC.setContextMap(savedContext)
                val fileChecksum      = results.head.asInstanceOf[Option[String]]
                val applianceChecksum = results(1).asInstanceOf[Try[String]]
                logger.debug(s"fileChecksum is $fileChecksum")
                logger.debug(s"applianceChecksum is $applianceChecksum")
                fileChecksum == applianceChecksum.toOption
              })

              checksumMatchFut.flatMap({
                case true => //checksums match
                  MDC.setContextMap(savedContext)
                  if (metadata.size() == file.size) { //file size and checksums match, no copy required
                    logger.info(s"Object with object id ${rec.objectId} and filepath ${fullPath} already exists")
                    Future(Right(rec.asJson))
                  } else {                            //checksum matches but size does not (unlikely but possible), new copy required
                    logger.info(s"Object with object id ${rec.objectId} and filepath $fullPath exists but size does not match, copying fresh version")
                    copyFile(vault, file, Some(rec))
                  }
                case false =>
                  MDC.setContextMap(savedContext)
                  //checksums don't match, size match undetermined, new copy required
                  logger.info(s"Object with object id ${rec.objectId} and filepath $fullPath exists but checksum does not match, copying fresh version")
                  copyFile(vault, file, Some(rec))
              })
            }).recoverWith({
              case err:java.io.IOException =>
                if(err.getMessage.contains("does not exist (error 306)")) {
                  copyFile(vault, file, Some(rec))
                } else {
                  //the most likely cause of this is that the sdk threw because the appliance is under heavy load and
                  //can't do the checksum at this time
                  logger.error(s"Error validating objectmatrix checksum: ${err.getMessage}", err)
                  Future(Left(s"ObjectMatrix error: ${err.getMessage}"))
                }
              case err:Throwable =>
                // Error contacting ObjectMatrix, log it and retry via the queue
                logger.info(s"Failed to get object from vault $fullPath: ${err.getMessage} for checksum, will retry")
                Future(Left(s"ObjectMatrix error: ${err.getMessage}"))
            })
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
          matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
            processFile(file, vault)
          }
        }
    }
  }
}
