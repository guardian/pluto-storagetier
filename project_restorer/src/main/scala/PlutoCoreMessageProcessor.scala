
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.gu.multimedia.mxscopy.{MXSConnectionBuilder, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue}
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{OnlineOutputMessage, ProjectUpdateMessage}
import org.slf4j.LoggerFactory

import java.time.ZonedDateTime
import scala.concurrent.{ExecutionContext, Future}


class PlutoCoreMessageProcessor(mxsConfig:MatrixStoreConfig)(implicit mat:Materializer,
                                                             matrixStoreBuilder: MXSConnectionBuilder,
                                                             ec:ExecutionContext)
  extends
  MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)


  def filesByProject(vault:Vault, projectId:String) = {
    val sinkFactory = Sink.seq[OnlineOutputMessage]
    Source.fromGraph(new OMFastContentSearchSource(vault,
      s"GNM_PROJECT_ID:\"$projectId\"",
      Array("MXFS_PATH","MXFS_FILENAME", "__mxs__length")
    )
    ).map(OnlineOutputMessage.apply)
      .toMat(sinkFactory)(Keep.right)
      .run()
  }


  def searchAssociatedMedia(projectId: Int, vault: Vault)  = {
   for {
     result <- filesByProject(vault, projectId.toString)
   } yield result
  }

  def handleStatusMessage(updateMessage: ProjectUpdateMessage):Future[Either[String, Json]] = {
       matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) {vault =>
        searchAssociatedMedia(updateMessage.id, vault).map(results=> {

          if(results.length < 10000 ){
            val msg = RestorerSummaryMessage(updateMessage.id, ZonedDateTime.now(), updateMessage.status, results.length, 0)
            Right(msg.asJson)
          }
          else {
            throw new RuntimeException("Too many files attached to project")
          }
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
    routingKey match {
      case "core.project.update" =>
        logger.info(s"Received message of $routingKey from queue: ${msg.noSpaces}")
        msg.as[ProjectUpdateMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ProjectUpdate: $err"))
          case Right(updateMessage) =>
            logger.info(s"here is an update status ${updateMessage.status}")
            handleStatusMessage(updateMessage)
        }
      case _ =>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
