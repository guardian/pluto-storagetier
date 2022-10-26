import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.implicits.toTraverseOps
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, MessageProcessorReturnValue}
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.vidispine.{VSOnlineOutputMessage, VidispineCommunicator}
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{InternalOnlineOutputMessage, ProjectUpdateMessage}
import org.slf4j.LoggerFactory

import java.time.ZonedDateTime
import scala.annotation.unused
import scala.concurrent.impl.Promise
import scala.concurrent.{ExecutionContext, Future}

class PlutoCoreMessageProcessor(mxsConfig: MatrixStoreConfig, asLookup: AssetFolderLookup)(implicit mat: Materializer,
                                                                                           matrixStoreBuilder: MXSConnectionBuilder,
                                                                                           vidispineCommunicator: VidispineCommunicator,
                                                                                           ec: ExecutionContext) extends MessageProcessor {

  private val logger = LoggerFactory.getLogger(getClass)

  private val statusesMediaNotRequired = List(EntryStatus.Held.toString, EntryStatus.Completed.toString, EntryStatus.Killed.toString)

  def searchAssociatedOnlineMedia(projectId: Int, vidispineCommunicator: VidispineCommunicator): Future[Seq[OnlineOutputMessage]] =
    onlineFilesByProject(vidispineCommunicator, projectId)

//<<<<<<< Updated upstream
  def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] =
    vidispineCommunicator.getFilesOfProject(projectId)
      .map(_
        .filterNot(isBranding)
        .filter(isDeletableInAllProjects)
        .map(item => InternalOnlineOutputMessage.toOnlineOutputMessage(item)))

  // GP-823 Ensure that branding does not get deleted
  def isBranding(item: VSOnlineOutputMessage): Boolean = item.mediaCategory.toLowerCase match {
    case "branding" => true // Case insensitive
    case _ => false
  }


  def isDeletableInAllProjects(item: VSOnlineOutputMessage): Boolean = {
    val eventualBoolean: Future[Boolean] = isDeletableInAllProjectsFut(item)
    eventualBoolean match {
      case promise: Promise.DefaultPromise[_] => ???
      case Future.never => ???
      case _ => ???
    }
  }

  def isDeletableInAllProjectsFut(item: VSOnlineOutputMessage): Future[Boolean] = {
    val future = item.projectIds.map(
      projectId => {
        val eventualMaybeRecord = asLookup.getProjectMetadata(projectId.toString)
        eventualMaybeRecord.flatMap({
          case Some(value) =>
            println(s">>> status for $projectId is ${value.status}")
//            Future(value.status == EntryStatus.InProduction)
            Future(Some(value.status))
          case None =>
            println(s">>> status for $projectId COULD NOT BE FOUND!")
            Future(None)
        })
    }).sequence

//    val new = statuses.map(_.collect({case Some(EntryStatus.New) => x}))
    val a = List(EntryStatus.Held, EntryStatus.New, EntryStatus.Killed)
    a.contains(EntryStatus.Completed) match {
      case true => println("hej")
      case _ => println("då")
    }
    val wha = future.map(seq => {
      isActivelyUsed(seq)
    })


    wha.map(x => println(x))
    wha.map(w => !w)




  }


  private def isActivelyUsed(seq: Seq[Option[EntryStatus.Value]]) = {
    val filtered = seq
      .collect({ case Some(x) => x })
      .filter(s => s == EntryStatus.Held)
      .filter(s => s == EntryStatus.Completed)
      .filter(s => s == EntryStatus.Killed)
    if (filtered.nonEmpty)
      true
    else
      false
  }

  def searchAssociatedNearlineMedia(projectId: Int, vault: Vault): Future[Seq[OnlineOutputMessage]] = {
    nearlineFilesByProject(vault, projectId.toString)
  }

  def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = {
    val sinkFactory = Sink.seq[OnlineOutputMessage]
    Source.fromGraph(new OMFastContentSearchSource(vault,
      s"GNM_PROJECT_ID:\"$projectId\"",
      Array("MXFS_PATH", "MXFS_FILENAME", "GNM_PROJECT_ID", "GNM_TYPE", "__mxs__length")
    )
    ).filterNot(isBranding)
      .filterNot(isMetadataOrProxy)
      .map(entry => InternalOnlineOutputMessage.toOnlineOutputMessage(entry))
      .toMat(sinkFactory)(Keep.right)
      .run()
  }

  // GP-823 Ensure that branding does not get deleted
  def isBranding(entry: ObjectMatrixEntry): Boolean = entry.stringAttribute("GNM_TYPE") match {
    case Some(gnmType) =>
      gnmType.toLowerCase match {
        case "branding" => true // Case sensitive
        case _ => false
      }
    case _ => false
  }

  // GP-826 Ensure that we don't emit Media not required-messages for proxy and metadata files, as media_remover uses
  // ATT_PROXY_OID and ATT_META_OID on the main file to remove those.
  // (This will just remove the latest version of the metadata file, but is a known limitation and deemed acceptable for now.)
  def isMetadataOrProxy(entry: ObjectMatrixEntry): Boolean = entry.stringAttribute("GNM_TYPE") match {
    case Some(gnmType) =>
      gnmType.toLowerCase match {
        case "metadata" => true
        case "proxy" => true
        case _ => false
      }
    case _ => false
  }

  def getNearlineResults(projectId: Int) = {
    matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
      searchAssociatedNearlineMedia(projectId, vault).map(Right.apply)
    }
  }

  def getOnlineResults(projectId: Int) = {
    searchAssociatedOnlineMedia(projectId, vidispineCommunicator).map(Right.apply)
  }

  def handleUpdateMessage(updateMessage: ProjectUpdateMessage, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    updateMessage.status match {
      case status if statusesMediaNotRequired.contains(status) =>
        Future
          .sequence(Seq(getNearlineResults(updateMessage.id), getOnlineResults(updateMessage.id)))
          .map(allResults => processResults(allResults, RoutingKeys.MediaNotRequired, framework, updateMessage.id, updateMessage.status))
      case _ => Future.failed(SilentDropMessage(Some(s"Incoming project update message has a status we don't care about (${updateMessage.status}), dropping it.")))
    }

  private def processResults(allResults: Seq[Either[String, Seq[OnlineOutputMessage]]], routingKey: String, framework: MessageProcessingFramework, projectId: Int, projectStatus: String) = (allResults.head, allResults(1)) match {
    case (Right(nearlineResults), Right(onlineResults)) =>
      if (nearlineResults.length < 10000 && onlineResults.length < 10000) {
        logger.info(s"About to send bulk messages for ${nearlineResults.length} nearline results")
        framework.bulkSendMessages(routingKey + ".nearline", nearlineResults)

        logger.info(s"About to send bulk messages for ${onlineResults.length} online results")
        framework.bulkSendMessages(routingKey + ".online", onlineResults)

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
            if (updateMessageList.length > 1) logger.error("Received multiple objects in one event, this is not supported. Events may be dropped.")

            updateMessageList.headOption match {
              case None =>
                Future.failed(new RuntimeException("The incoming event was empty"))
              case Some(updateMessage: ProjectUpdateMessage) =>
                handleUpdateMessage(updateMessage, msgProcessingFramework)
            }
        }
      case _ =>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
