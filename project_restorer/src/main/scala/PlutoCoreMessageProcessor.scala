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
      .map(_.filterNot(isBranding).map(InternalOnlineOutputMessage.toOnlineOutputMessage))
  }

  def enhanceOnlineResultsWithCrosslinkStatus(items: Either[String, Seq[OnlineOutputMessage]]): Future[Either[String, Seq[(SendRemoveActionTarget.Value, OnlineOutputMessage)]]] =
    items match {
      case Left(err) => Future(Left(err))
      case Right(onlineOutputMessages) =>
        onlineOutputMessages.map(isDeletableInAllProjectsFut).sequence.map(Right.apply)
    }

  def isDeletableInAllProjectsFut(item: OnlineOutputMessage): Future[(SendRemoveActionTarget.Value, OnlineOutputMessage)] = {
    // The first project id is the id of the triggering project, so we don't need to check the status of that
    val otherProjectIds = getCrosslinkProjectIds(item)
    logger.debug(s"All project for ${item.vidispineItemId.getOrElse("<missing vidispineItemId>")}: ${item.projectIds}, getting statuses for $otherProjectIds")

    getStatusesForProjects(otherProjectIds).map(statusMaybes =>
      statusMaybes.collect({ case Some(value) => value }) match {
        case crosslinkedProjectStatuses if stillInUse(crosslinkedProjectStatuses) => (SendRemoveActionTarget.Neither, item)
        case crosslinkedProjectStatuses if isHeld(crosslinkedProjectStatuses) => (SendRemoveActionTarget.OnlyOnline, item)
        case crosslinkedProjectStatuses if releasedByAll(crosslinkedProjectStatuses) => (SendRemoveActionTarget.Both, item)
        case _ => (SendRemoveActionTarget.Neither, item)
      })
  }

  private def getCrosslinkProjectIds(item: OnlineOutputMessage) = {
    // The first project id is the id of the triggering project, so we don't need to check the status of that
    item.projectIds.filterNot(_ == item.projectIds.head)
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
      .filterNot(isMetadataOrProxy) // TODO Figure out if we should filter these out or not, given that they DO work for files that do not need to be on Deep Archive to be deletable
      .map(InternalOnlineOutputMessage.toOnlineOutputMessage)
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

  private def getNearlineResultsIfNotHeld(projectId: Int, projectStatus: String): Future[Either[String, Seq[OnlineOutputMessage]]] =
    if (projectStatus == EntryStatus.Held.toString) {
      logger.debug(s"Status of project ${projectId} is ${EntryStatus.Held}, we won't consider any nearline files")
      Future(Right(Seq()))
    } else {
      getNearlineResults(projectId)
    }

  def getNearlineResults(projectId: Int): Future[Either[String, Seq[OnlineOutputMessage]]] =
    matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
      searchAssociatedNearlineMedia(projectId, vault).map(Right.apply)
    }

  def getOnlineResults(projectId: Int): Future[Either[String, Seq[OnlineOutputMessage]]] = {
    searchAssociatedOnlineMedia(vidispineCommunicator, projectId).map(Right.apply)
  }

  def handleUpdateMessage(updateMessage: ProjectUpdateMessage, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    updateMessage.status match {
      case status if statusesMediaNotRequired.contains(status) =>
        (for {
          onlineResults <- getOnlineResults(updateMessage.id)
          nearlineResults <- getNearlineResultsIfNotHeld(updateMessage.id, updateMessage.status)
          crosslinkStatusItemTuples <- enhanceOnlineResultsWithCrosslinkStatus(onlineResults)
          _ = logOutBasis(updateMessage, crosslinkStatusItemTuples)
          filteredSeqs = crosslinkFilter(onlineResults, nearlineResults, crosslinkStatusItemTuples)
        } yield filteredSeqs)
          .map(filteredSeqs => processResults(filteredSeqs, RoutingKeys.MediaNotRequired, framework, updateMessage.id, updateMessage.status))

      case _ => Future.failed(SilentDropMessage(Some(s"Incoming project update message has a status we don't care about (${updateMessage.status}), dropping it.")))
    }

  private def processResults(allResults: Seq[Either[String, Seq[OnlineOutputMessage]]], routingKey: String, framework: MessageProcessingFramework, projectId: Int, projectStatus: String): Either[String, MessageProcessorReturnValue] =
    (allResults.head, allResults(1), allResults(2)) match {
      case (Right(onlineResults), Right(nearlineResults), Right(_)) =>
        if (nearlineResults.length < 20000 && onlineResults.length < 20000) {
          logger.info(s"About to send bulk messages for ${nearlineResults.length} filtered nearline results")
          framework.bulkSendMessages(routingKey + ".nearline", nearlineResults)

          logger.info(s"About to send bulk messages for ${onlineResults.length} filtered online results")
          framework.bulkSendMessages(routingKey + ".online", onlineResults)

          logger.info(s"Bulk messages sent; about to send the RestorerSummaryMessage for project $projectId")
          val msg = RestorerSummaryMessage(projectId, ZonedDateTime.now(), projectStatus, numberOfAssociatedFilesNearline = nearlineResults.length, numberOfAssociatedFilesOnline = onlineResults.length)

          Right(MessageProcessorConverters.contentToMPRV(msg.asJson))
        } else {
          throw new RuntimeException(s"Too many files attached to project $projectId, nearlineResults = ${nearlineResults.length}, onlineResults = ${onlineResults.length}")
        }
      case (_, _, Left(crosslinkErr)) =>
        logger.error(s"Could not filter for crosslinked projects: $crosslinkErr")
        Left(s"Could not filter for crosslinked projects: $crosslinkErr")
      case (Left(onlineErr), _, _) =>
        logger.error(s"Unexpected error from getOnlineResults: $onlineErr")
        Left(s"Unexpected error from getOnlineResults: $onlineErr")
      case (_, Left(nearlineErr), _) =>
        logger.error(s"Could not connect to Matrix store for nearline results: $nearlineErr")
        Left(s"Could not connect to Matrix store for nearline results: $nearlineErr")
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
  override def handleMessage(routingKey: String, msg: Json, msgProcessingFramework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
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

  private def logPreAndPostCrosslinkFiltering(onlineResults: Seq[OnlineOutputMessage], nearlineResults: Seq[OnlineOutputMessage], filteredNearline: Seq[OnlineOutputMessage], filteredOnline: Seq[OnlineOutputMessage]): Unit = {
    if (nearlineResults.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY)
      logger.debug(s"${nearlineResults.size} nearline results: " + nearlineResults.map(nearline => s"${nearline.nearlineId.getOrElse("<missing>")}|${nearline.originalFilePath.getOrElse("<no path>")}").mkString(", "))
    else
      logger.debug(s"${nearlineResults.size} nearline results")

    if (filteredNearline.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY)
      logger.debug(s"${filteredNearline.size} filtered nearline results: ${filteredNearline.map(_.nearlineId.getOrElse("<missing>"))}")
    else
      logger.debug(s"${nearlineResults.size} filtered nearline results")

    if (onlineResults.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY)
      logger.debug(s"${onlineResults.size} online results: ${onlineResults.map(_.vidispineItemId.getOrElse("<missing>"))}")
    else
      logger.debug(s"${onlineResults.size} online results")

    if (filteredOnline.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY)
      logger.debug(s"${filteredOnline.size} filtered online results: ${filteredOnline.map(_.vidispineItemId.getOrElse("<missing>"))}")
    else
      logger.debug(s"${filteredOnline.size} filtered online results")
  }

  private def logOutBasis(updateMessage: ProjectUpdateMessage, crosslinkstatusItemTuplesE: Either[String, Seq[(PlutoCoreMessageProcessor.SendRemoveActionTarget.Value, OnlineOutputMessage)]]): Unit =
    crosslinkstatusItemTuplesE match {
      case Left(err) => logger.warn("")
      case Right(crosslinkstatusItemTuples) =>
        if (crosslinkstatusItemTuples.size < MAX_ITEMS_TO_LOG_INDIVIDUALLY) {
          logger.debug(s"Basis for filtering items for project ${updateMessage.id} (online items as vsItemId|nearlineId|path|crosslinkedIds->SendRemoveActionTarget) " +
            crosslinkstatusItemTuples.map(
              t =>
                s"${t._2.vidispineItemId.getOrElse("<no vs ID>")}|" +
                  s"${t._2.nearlineId.getOrElse("<no online ID>")}|" +
                  s"${t._2.originalFilePath.getOrElse("<no path>")}|" +
                  s"${getCrosslinkProjectIds(t._2)}->" +
                  s"${t._1}"
            ).mkString(", "))
        } else {
          logger.debug(s"${crosslinkstatusItemTuples.size} crosslink/onlineItem tuples, too many to list all in log")
        }
    }

  def crosslinkFilter(onlineResultsE: Either[String, Seq[OnlineOutputMessage]], nearlineResultsE: Either[String, Seq[OnlineOutputMessage]], crosslinkStatusItemTuplesE: Either[String, Seq[(PlutoCoreMessageProcessor.SendRemoveActionTarget.Value, OnlineOutputMessage)]]): Seq[Either[String, Seq[OnlineOutputMessage]]] =
    (onlineResultsE, nearlineResultsE, crosslinkStatusItemTuplesE) match {
      case (Right(onlineResults), Right(nearlineResults), Right(crosslinkStatusItemTuples)) =>
        val neitherItemTuples = crosslinkStatusItemTuples.collect({ case t if SendRemoveActionTarget.Neither == t._1 => t })
        val onlyOnlineItemTuples = crosslinkStatusItemTuples.collect({ case t if SendRemoveActionTarget.OnlyOnline == t._1 => t })

        val neitherNearlineIds = neitherItemTuples.map(_._2.nearlineId).collect({ case Some(x) => x })
        val onlyOnlineNearlineIds = onlyOnlineItemTuples.map(_._2.nearlineId).collect({ case Some(x) => x })
        val filteredNearline = nearlineResults
          .filterNot(item => neitherNearlineIds.contains(item.nearlineId.getOrElse("<missing nearline ID>")))
          .filterNot(item => onlyOnlineNearlineIds.contains(item.nearlineId.getOrElse("<missing nearline ID>")))

        val neitherOnlineIds = neitherItemTuples.map(_._2.vidispineItemId).collect({ case Some(x) => x })
        val filteredOnline = crosslinkStatusItemTuples
          .filterNot(item => neitherOnlineIds.contains(item._2.vidispineItemId.getOrElse("<missing vsItem ID>"))).map(_._2)

        logPreAndPostCrosslinkFiltering(onlineResults, nearlineResults, filteredNearline, filteredOnline)

        Seq(Right(filteredOnline), Right(filteredNearline), Right(crosslinkStatusItemTuples.map(_._2)))

      case (onlineE, nearlineE, Right(_)) =>
        Seq(onlineE, nearlineE, Right(Seq()))

      case (onlineE, nearlineE, Left(err)) =>
        Seq(onlineE, nearlineE, Left(err))
    }

}

object PlutoCoreMessageProcessor {
  object SendRemoveActionTarget extends Enumeration {
    val Neither, OnlyOnline, Both = Value
  }
}
