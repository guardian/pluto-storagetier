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
import PlutoCoreMessageProcessor.SendRemoveActionTarget
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{InternalOnlineOutputMessage, ProjectUpdateMessage}
import org.slf4j.LoggerFactory
import cats.implicits._

import java.time.ZonedDateTime
import scala.concurrent.{ExecutionContext, Future}

class PlutoCoreMessageProcessor(mxsConfig: MatrixStoreConfig, asLookup: AssetFolderLookup)(implicit mat: Materializer,
                                                                                           matrixStoreBuilder: MXSConnectionBuilder,
                                                                                           vidispineCommunicator: VidispineCommunicator,
                                                                                           ec: ExecutionContext) extends MessageProcessor {

  private val MAX_ITEMS_TO_LOG_INDIVIDUALLY = 100

  private val logger = LoggerFactory.getLogger(getClass)

  private val statusesMediaNotRequired = List(EntryStatus.Held.toString, EntryStatus.Completed.toString, EntryStatus.Killed.toString)

  def searchAssociatedOnlineMedia(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] =
    onlineFilesByProject(vidispineCommunicator, projectId)

  def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = {
    vidispineCommunicator.getFilesOfProject(projectId)
      .map(_.filterNot(isBranding))
      .map(_.map(item => InternalOnlineOutputMessage.toOnlineOutputMessage(item)))
  }

  def enhanceOnlineResultsWithCrosslinkStatus(items: Seq[OnlineOutputMessage]): Future[Seq[(SendRemoveActionTarget.Value, OnlineOutputMessage)]] =
    items.map(isDeletableInAllProjectsFut).sequence

  def isDeletableInAllProjectsFut(item: OnlineOutputMessage): Future[(SendRemoveActionTarget.Value, OnlineOutputMessage)] = {
    // The first project id is the id of the triggering project, so we don't need to check the status of that
    val otherProjectIds = item.projectIds.filterNot(_ == item.projectIds.head)

    getStatusesForProjects(otherProjectIds).map(statusMaybes =>
      statusMaybes.collect({ case Some(value) => value }) match {
        case crosslinkedProjectStatuses if stillInUse(crosslinkedProjectStatuses) => (SendRemoveActionTarget.Neither, item)
        case crosslinkedProjectStatuses if isHeld(crosslinkedProjectStatuses) => (SendRemoveActionTarget.OnlyOnline, item)
        case crosslinkedProjectStatuses if releasedByAll(crosslinkedProjectStatuses) => (SendRemoveActionTarget.Both, item)
        case _ => (SendRemoveActionTarget.Neither, item)
      })
  }

  private def getStatusesForProjects(otherProjectIds: Seq[String]) =
    otherProjectIds.map(
      projectId => {
        asLookup.getProjectMetadata(projectId).flatMap({
          case Some(value) => Future(Some(value.status))
          case None =>
            logger.warn(s"Status for $projectId could not be found")
            Future(None)
        })
      }).sequence


  // GP-823 Ensure that branding does not get deleted
  def isBranding(item: VSOnlineOutputMessage): Boolean = item.mediaCategory.toLowerCase match {
    case "branding" => true // Case insensitive
    case _ => false
  }

  def stillInUse(someStatuses: Seq[EntryStatus.Value]): Boolean =
    someStatuses.nonEmpty && !someStatuses.forall(s => List(EntryStatus.Held, EntryStatus.Completed, EntryStatus.Killed).contains(s))

  def isHeld(someStatuses: Seq[EntryStatus.Value]): Boolean =
    someStatuses.contains(EntryStatus.Held) && !someStatuses.contains(EntryStatus.InProduction) && !someStatuses.contains(EntryStatus.New)

  def releasedByAll(someStatuses: Seq[EntryStatus.Value]): Boolean =
    someStatuses.isEmpty || someStatuses.forall(s => List(EntryStatus.Completed, EntryStatus.Killed).contains(s))

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
      // .filterNot(isMetadataOrProxy) // TODO Figure out if we should filter these out or not, given that they DO work for files that do not need to be on Deep Archive to be deletable
      .map(entry => InternalOnlineOutputMessage.toOnlineOutputMessage(entry))
      .toMat(sinkFactory)(Keep.right)
      .run()
  }

  // GP-823 Ensure that branding does not get deleted
  def isBranding(entry: ObjectMatrixEntry): Boolean = entry.stringAttribute("GNM_TYPE") match {
    case Some(gnmType) =>
      gnmType.toLowerCase match {
        case "branding" => true // Case insensitive
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

  def getNearlineResults(projectId: Int): Future[Either[String, Seq[OnlineOutputMessage]]] =
    matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
      searchAssociatedNearlineMedia(projectId, vault).map(Right.apply)
    }

  def getOnlineResults(projectId: Int): Future[Right[Nothing, Seq[OnlineOutputMessage]]] = {
    searchAssociatedOnlineMedia(vidispineCommunicator, projectId).map(Right.apply)
  }

  def handleUpdateMessage(updateMessage: ProjectUpdateMessage, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    updateMessage.status match {
      case status if statusesMediaNotRequired.contains(status) =>

        getOnlineResults(updateMessage.id).flatMap({
          case Right(onlineResults: Seq[OnlineOutputMessage]) =>

            getNearlineResults(updateMessage.id).flatMap({
              case Right(nearlineResults) =>

                enhanceOnlineResultsWithCrosslinkStatus(onlineResults).map(crosslinkstatusItemTuples => {
                  if (crosslinkstatusItemTuples.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY) {
                    logger.debug("Basis for filtering (vsItemId/onlineId->SendRemoveActionTarget): " + crosslinkstatusItemTuples.map(t => s"${t._2.vidispineItemId.getOrElse("<no vs ID>")}/${t._2.nearlineId.getOrElse("<no online ID>")}->${t._1}").mkString(", "))
                  } else {
                    logger.debug(s"${crosslinkstatusItemTuples.size} crosslink/onlineItem tuples, too many to list all in log")
                  }

                  val neitherItemTuples = crosslinkstatusItemTuples.collect({ case t if SendRemoveActionTarget.Neither == t._1 => t })
                  val onlyOnlineItemTuples = crosslinkstatusItemTuples.collect({ case t if SendRemoveActionTarget.OnlyOnline == t._1 => t })

                  val neitherNearlineIds = neitherItemTuples.map(_._2.nearlineId).collect({ case Some(x) => x })
                  val onlyOnlineNearlineIds = onlyOnlineItemTuples.map(_._2.nearlineId).collect({ case Some(x) => x })
                  val filteredNearline = nearlineResults
                    .filterNot(item => neitherNearlineIds.contains(item.nearlineId.getOrElse("<missing nearline ID>")))
                    .filterNot(item => onlyOnlineNearlineIds.contains(item.nearlineId.getOrElse("<missing nearline ID>")))

                  val neitherOnlineIds = neitherItemTuples.map(_._2.vidispineItemId).collect({ case Some(x) => x })
                  val filteredOnline = crosslinkstatusItemTuples
                    .filterNot(item => neitherOnlineIds.contains(item._2.vidispineItemId.getOrElse("<missing vsItem ID>"))).map(_._2)

                  logPreAndPostCrosslinkFiltering(onlineResults, nearlineResults, filteredNearline, filteredOnline)

                  processResults(filteredNearline, filteredOnline, RoutingKeys.MediaNotRequired, framework, updateMessage.id, updateMessage.status)
                })

              case Left(err) =>
                logger.error(s"Could not connect to Matrix store for nearline results: $err")
                Future(Left(s"Could not connect to Matrix store for nearline results: $err"))
            })
          case _ =>
            logger.error(s"Unexpected error from getOnlineResults")
            Future(Left(s"Unexpected error from getOnlineResults"))
        })

      case _ => Future.failed(SilentDropMessage(Some(s"Incoming project update message has a status we don't care about (${updateMessage.status}), dropping it.")))
    }

  private def processResults(nearlineResults: Seq[OnlineOutputMessage], onlineResults: Seq[OnlineOutputMessage], routingKey: String, framework: MessageProcessingFramework, projectId: Int, projectStatus: String): Either[String, MessageProcessorReturnValue] =
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
  private def logPreAndPostCrosslinkFiltering(onlineResults: Seq[OnlineOutputMessage], nearlineResults: Seq[OnlineOutputMessage], filteredNearline: Seq[OnlineOutputMessage], filteredOnline: Seq[OnlineOutputMessage]) = {
    if (nearlineResults.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY) logger.debug(s"nearlineResults: ${nearlineResults.map(_.nearlineId.getOrElse("<missing>"))}") else logger.debug(s"${nearlineResults.size} nearline results")
    if (filteredNearline.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY) logger.debug(s"filteredNearlineResults: ${filteredNearline.map(_.nearlineId.getOrElse("<missing>"))}") else logger.debug(s"${nearlineResults.size} filtered nearline results")
    if (onlineResults.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY) logger.debug(s"onlineResults: ${onlineResults.map(_.vidispineItemId.getOrElse("<missing>"))}") else logger.debug(s"${onlineResults.size} online results")
    if (filteredOnline.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY) logger.debug(s"filteredOnlineResults: ${filteredOnline.map(_.vidispineItemId.getOrElse("<missing>"))}") else logger.debug(s"${filteredOnline.size} filtered online results")
  }
}

object PlutoCoreMessageProcessor {
  object SendRemoveActionTarget extends Enumeration {
    val Neither, OnlyOnline, Both = Value
  }
}
