import Main.{actorSystem, mat}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue}
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.ProjectUpdateMessage
import org.slf4j.LoggerFactory

import scala.Console.err
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class PlutoCoreMessageProcessor(mxsConfig:MatrixStoreConfig)(implicit mxsConnectionBuilder:MXSConnectionBuilderImpl)
  extends
  MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)


  def filesByProject(vault:Vault, projectId:String)(implicit mat:Materializer) = {
    val sinkFactory = Sink.seq[ObjectMatrixEntry]
    Source.fromGraph(new OMFastContentSearchSource(vault,
      s"GNM_PROJECT_ID:\"$projectId\"",
      Array("MXFS_PATH","MXFS_PATH","MXFS_FILENAME", "__mxs__length")
    )
    ).toMat(sinkFactory)(Keep.right)
      .run()
  }


  def searchAssociatedMedia(project_id: Int, vault: Vault) = {

    case Right(updateMessage)=>
     implicit val listOfFiles = filesByProject(vault, project_id.toString)
      return listOfFiles
    case Left(err)=>
      Future.failed(new RuntimeException("Failed to get status"))


  }

  def handleStatusMessage(updateMessage: ProjectUpdateMessage): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"here is an update status ${updateMessage.status}")
    case Right(updateMessage)=>
      mxsConnectionBuilder.withVaultFuture(mxsConfig.nearlineVaultId) {vault =>
        searchAssociatedMedia(updateMessage.id, vault)
      }

    case Left(err)=>
      Future.failed(new RuntimeException("Failed to get status"))

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
    logger.info(s"Received message of $routingKey from queue: ${msg.noSpaces}")
    routingKey match {
      case "core.project.update" =>
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
