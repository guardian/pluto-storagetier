
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, MessageProcessorConverters, MessageProcessorReturnValue}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
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
                                                             vidispineCommunicator: VidispineCommunicator,
                                                             ec:ExecutionContext) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  def searchAssociatedOnlineMedia(projectId: Int, vidispineCommunicator: VidispineCommunicator): Future[Seq[OnlineOutputMessage]] = {
    onlineFilesByProject(vidispineCommunicator, projectId)
  }

  def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = {
    vidispineCommunicator.getFilesOfProject(projectId)
      .map(_.map(OnlineOutputMessage.apply))
  }

  def searchAssociatedNearlineMedia(projectId: Int, vault: Vault): Future[Seq[OnlineOutputMessage]] = {
   for {
     result <-
       nearlineFilesByProject(vault, projectId.toString)
   } yield result
  }

  def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = {
    val sinkFactory = Sink.seq[OnlineOutputMessage]
    Source.fromGraph(new OMFastContentSearchSource(vault,
      s"GNM_PROJECT_ID:\"$projectId\"",
      Array("MXFS_PATH","MXFS_FILENAME", "__mxs__length")
    )
    ).map(OnlineOutputMessage.apply)
      .toMat(sinkFactory)(Keep.right)
      .run()
  }

  def getNearLineResults(projectId: Int) = {
    matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
      searchAssociatedNearlineMedia(projectId, vault).map(Right.apply)
    }
  }

  def getOnlineResults(projectId: Int) = {
    searchAssociatedOnlineMedia(projectId, vidispineCommunicator).map(Right.apply)
  }

  def handleStatusMessage(updateMessage: ProjectUpdateMessage, routingKey: String, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] = {
    Future.sequence(Seq(getNearLineResults(updateMessage.id), getOnlineResults(updateMessage.id))).map(allResults => {
      (allResults.head, allResults(1)) match {
        case (Right(nearlineResults), Right(onlineResults)) =>
          if (nearlineResults.length < 10000 && onlineResults.length < 10000) {
            framework.bulkSendMessages(routingKey, nearlineResults)
            framework.bulkSendMessages(routingKey, onlineResults)
            val msg = RestorerSummaryMessage(updateMessage.id, ZonedDateTime.now(), updateMessage.status, numberOfAssociatedFilesNearline = nearlineResults.length, numberOfAssociatedFilesOnline = onlineResults.length)
            Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
          } else {
            throw new RuntimeException(s"Too many files attached to project ${updateMessage.id}, nearlineResults = ${nearlineResults.length}, onlineResults = ${onlineResults.length}")
          }
        case (Left(onlineErr), _)=>
          logger.error("Could not connect to Matrix store: $onlineErr")
          Left(onlineErr)
      }
    })
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
  override def handleMessage(routingKey: String, msg: Json, msgProcessingFramework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] = {
    routingKey match {
      case "core.project.update" =>
        logger.info(s"Received message of $routingKey from queue: ${msg.noSpaces}")
        msg.as[ProjectUpdateMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ProjectUpdate: $err"))
          case Right(updateMessage) =>
            logger.info(s"here is an update status ${updateMessage.status}")
            handleStatusMessage(updateMessage, routingKey, msgProcessingFramework)
        }
      case _ =>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
