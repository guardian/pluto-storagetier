
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.plutocore.EntryStatus
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

  private val statusesMediaNotRequired = List(EntryStatus.Held.toString, EntryStatus.Completed.toString, EntryStatus.Killed.toString)

  def searchAssociatedOnlineMedia(projectId: Int, vidispineCommunicator: VidispineCommunicator): Future[Seq[OnlineOutputMessage]] = {
    onlineFilesByProject(vidispineCommunicator, projectId)
  }

  def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = {
    vidispineCommunicator.getFilesOfProject(projectId)
      .map(_.map(OnlineOutputMessage.apply))
  }

  def searchAssociatedNearlineMedia(projectId: Int, vault: Vault): Future[Seq[OnlineOutputMessage]] = {
    nearlineFilesByProject(vault, projectId.toString)
  }

  def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = {
    val sinkFactory = Sink.seq[OnlineOutputMessage]
    Source.fromGraph(new OMFastContentSearchSource(vault,
      s"GNM_PROJECT_ID:\"$projectId\"",
      Array("MXFS_PATH","MXFS_FILENAME", "GNM_PROJECT_ID", "GNM_TYPE", "__mxs__length")
    )
    ).map(OnlineOutputMessage.apply)
      .toMat(sinkFactory)(Keep.right)
      .run()
  }

  def getNearlineResults(projectId: Int) = {
    matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
      searchAssociatedNearlineMedia(projectId, vault).map(Right.apply)
    }
  }

  def getOnlineResults(projectId: Int) = {
    searchAssociatedOnlineMedia(projectId, vidispineCommunicator).map(Right.apply)
  }

  def handleUpdateMessage(updateMessage: ProjectUpdateMessage, routingKey: String, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    updateMessage.status match {
      case status if statusesMediaNotRequired.contains(status)  =>
        Future.sequence(Seq(getNearlineResults(updateMessage.id), getOnlineResults(updateMessage.id)))
          .map(allResults => processResults(allResults, RoutingKeys.MediaNotRequired, framework, updateMessage.id, updateMessage.status))
      case _ => Future.failed(SilentDropMessage(Some(s"Incoming project update message has a status we don't care about (${updateMessage.status}), dropping it.")))
    }

  private def processResults(allResults: Seq[Either[String, Seq[OnlineOutputMessage]]], routingKey: String, framework: MessageProcessingFramework, projectId: Int, projectStatus: String) = (allResults.head, allResults(1)) match {
    case (Right(nearlineResults), Right(onlineResults)) =>
      if (nearlineResults.length < 10000 && onlineResults.length < 10000) {
        logger.info(s"About to send bulk messages for ${nearlineResults.length} nearline results")
        framework.bulkSendMessages(routingKey, nearlineResults)

        logger.info(s"About to send bulk messages for ${onlineResults.length} online results")
        framework.bulkSendMessages(routingKey, onlineResults)

        logger.info(s"Bulk messages sent; about to send the RestorerSummaryMessage for project $projectId")
        val msg = RestorerSummaryMessage(projectId, ZonedDateTime.now(), projectStatus, numberOfAssociatedFilesNearline = nearlineResults.length, numberOfAssociatedFilesOnline = onlineResults.length)

        Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
      } else {
        throw new RuntimeException(s"Too many files attached to project $projectId, nearlineResults = ${nearlineResults.length}, onlineResults = ${onlineResults.length}")
      }
    case (Left(nearlineErr), _) =>
      logger.error(s"Could not connect to Matrix store for nearline results: $nearlineErr")
      Left(nearlineErr)
    case (_, Left(onlineErr)) =>
      logger.error(s"Unexpected error from getOnlineResults: $onlineErr")
      Left(onlineErr)
    case (Left(nearlineErr), Left(onlineErr)) =>
      logger.error(s"Could not connect to Matrix store for nearline results: $nearlineErr. ALSO, unexpected error from getOnlineResults: $onlineErr")
      Left(s"nearlineErr: $nearlineErr; onlineErr: $onlineErr")
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
        msg.as[Seq[ProjectUpdateMessage]] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ProjectUpdate: $err"))
          case Right(updateMessageList) =>
            logger.info(s"here is an update status ${updateMessageList.headOption.map(_.status)}")
            if(updateMessageList.length>1) logger.error("Received multiple objects in one event, this is not supported. Events may be dropped.")

            updateMessageList.headOption match {
              case None=>
                Future.failed(new RuntimeException("The incoming event was empty"))
              case Some(updateMessage: ProjectUpdateMessage)=>
                handleUpdateMessage(updateMessage, routingKey, msgProcessingFramework)
            }
        }
      case _ =>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
