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
import PlutoCoreMessageProcessor.SendRemoveAction
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{InternalOnlineOutputMessage, ProjectUpdateMessage}
import org.slf4j.LoggerFactory
import cats.implicits._

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



  def searchAssociatedOnlineMedia(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] =
    onlineFilesByProject(vidispineCommunicator, projectId)

  def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = {
    vidispineCommunicator.getFilesOfProject(projectId)
      .map(_.filterNot(isBranding))
      .map(_.map(item => InternalOnlineOutputMessage.toOnlineOutputMessage(item)))
  }


  private def toBucketsF(a: Future[Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]]) = {
    val nayes: Future[Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]] = a.map(_.collect({ case n if SendRemoveAction.No == n._1 => n }))
    val ayes = a.map(_.collect({ case a if SendRemoveAction.Yes == a._1 => a }))
    val onlines = a.map(_.collect({ case o if SendRemoveAction.OnlyOnline == o._1 => o }))

    nayes.map(_.foreach(r => println(s"nayes: ${r._2.projectIds} => ${r._1}")))
    ayes.map(_.foreach(r => println(s"ayes: ${r._2.projectIds} => ${r._1}")))
    onlines.map(_.foreach(r => println(s"onlines: ${r._2.projectIds} => ${r._1}")))
    val buckets = (nayes, ayes, onlines)
    buckets
  }
  private def toBuckets(a: Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]) = {
    val nayes: Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)] = a.collect({ case n if SendRemoveAction.No == n._1 => n })
    val ayes = a.collect({ case a if SendRemoveAction.Yes == a._1 => a })
    val onlines = a.collect({ case o if SendRemoveAction.OnlyOnline == o._1 => o })

    nayes.foreach(r => println(s"nayes: ${r._2.projectIds} => ${r._1}"))
    ayes.foreach(r => println(s"ayes: ${r._2.projectIds} => ${r._1}"))
    onlines.foreach(r => println(s"onlines: ${r._2.projectIds} => ${r._1}"))
    val buckets = (nayes, ayes, onlines)
    buckets
  }

  def searchAssociatedOnlineMedia3(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[(SendRemoveAction.Value, OnlineOutputMessage)]] = {
    val a = onlineFilesByProject3(vidispineCommunicator, projectId)
    val buckets = toBucketsF(a)
    buckets._1
    a
  }

  def onlineFilesByProject3(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[(SendRemoveAction.Value, OnlineOutputMessage)]] = {

    val itemsFut: Future[Seq[OnlineOutputMessage]] = vidispineCommunicator.getFilesOfProject(projectId,100)
      .map(_.filterNot(isBranding))
      .map(_.map(item => InternalOnlineOutputMessage.toOnlineOutputMessage(item)))

    itemsFut.map(_.map(i => println(s"§§ $i")))

    val sendRemoveActionItemTuplesFut = itemsFut.map(
      _.map(
        item => isDeletableInAllProjectsFut(item)
      ).sequence
    ).flatten

    sendRemoveActionItemTuplesFut.map(_.foreach(r => println(s"w4: ${r._2.projectIds} => ${r._1}")))

    sendRemoveActionItemTuplesFut
  }

  //  def onlineFilesByProject2(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[(SendRemoveAction.Value, VSOnlineOutputMessage)]] = {
//    val items: Future[Seq[VSOnlineOutputMessage]] = vidispineCommunicator.getFilesOfProject(projectId).map(_.filterNot(isBranding))
//    //    items
//    //        .filter(isDeletableInAllProjects)
//    //        items.map(_.map(item => InternalOnlineOutputMessage.toOnlineOutputMessage(item)))
////    items.map(
////      _.map((item: VSOnlineOutputMessage) => isDeletableInAllProjectsFut(item)))
//
//    items.map(
//      _.map(
//        item => isDeletableInAllProjectsFut(item)
//      ).sequence
//    ).flatten
//
//
//  //    assetFolderRecordLookup(forFile)
////      .flatMap(
////        _.map(record => getProjectMetadata(record.project)
////        ).sequence.map(_.flatten) //.sequence here is a bit of cats "magic" that turns the Option[Future[Option]] into a Future[Option[Option]]
////      )
//
//  }

  def getisdel(items: Future[Seq[OnlineOutputMessage]] ) = {
    items.map(
      _.map(
        item => isDeletableInAllProjectsFut(item)
      ).sequence
    ).flatten
  }

  def isDeletableInAllProjectsFut(item: OnlineOutputMessage): Future[(SendRemoveAction.Value, OnlineOutputMessage)] = {
    val otherProjectIds = item.projectIds.filterNot(_ == item.projectIds.head)

    println(s"projectIds: ${item.projectIds}")
    println(s"otherProjectIds: $otherProjectIds")

    getStatusesForProjects(otherProjectIds).map {
      case s if stillInUse(s) => (SendRemoveAction.No, item)
      case t if lowestIsHeld(t) => (SendRemoveAction.OnlyOnline, item)
      case u if releasedByAll(u) => (SendRemoveAction.Yes, item)
      case _ => (SendRemoveAction.No, item)
    }
  }

  private def getStatusesForProjects(otherProjectIds: Seq[String]) =
    otherProjectIds.map(
      projectId => {
        val maybeRecordFut = asLookup.getProjectMetadata(projectId)
        maybeRecordFut.flatMap({
          case Some(value) =>
            println(s">>> status for $projectId is ${value.status}")
            Future(Some(value.status))
          case None =>
            println(s">>> status for $projectId COULD NOT BE FOUND!")
            Future(None)
        })
      }).sequence


  //<<<<<<< Updated upstream
//  def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = {
//    val future: Future[Seq[OnlineOutputMessage]] = vidispineCommunicator.getFilesOfProject(projectId)
//      .map(_
//        .filterNot(isBranding)
////        .filter(isDeletableInAllProjectsFut)
//        .map((item: VSOnlineOutputMessage) => InternalOnlineOutputMessage.toOnlineOutputMessage(item)))
//      )
//    future.flatMap(_.is)
//  }


  // GP-823 Ensure that branding does not get deleted
  def isBranding(item: VSOnlineOutputMessage): Boolean = item.mediaCategory.toLowerCase match {
    case "branding" => true // Case insensitive
    case _ => false
  }


//  def isDeletableInAllProjects(item: VSOnlineOutputMessage): Boolean = {
//    val eventualBoolean: Future[Boolean] = isDeletableInAllProjectsFut(item)
//    eventualBoolean.onComplete(_.get)
//  }



  /**
   * If there is something other than Held, Completed, Killed
   *
   * @param seq
   * @return true if any project is still using it
   */
   def stillInUse(seq: Seq[Option[EntryStatus.Value]]): Boolean = {
     val someStatuses = seq.collect({ case Some(x) => x })
     val bool = someStatuses.nonEmpty && !someStatuses.forall(s => List(EntryStatus.Held, EntryStatus.Completed, EntryStatus.Killed).contains(s))
     println(s"stillInUse: someStatuses: $someStatuses => $bool")
     bool
   }

  /**
   * If there's nothing but Held
   * @param seq
   * @return
   */
  def lowestIsHeld(seq: Seq[Option[EntryStatus.Value]]): Boolean = {
    val someStatuses = seq.collect({ case Some(x) => x })
    someStatuses.contains(EntryStatus.Held) && !someStatuses.exists(s => s != EntryStatus.Held)
  }

  def releasedByAll(seq: Seq[Option[EntryStatus.Value]]): Boolean = {
    val someStatuses = seq.collect({ case Some(x) => x })
    someStatuses.isEmpty || someStatuses.forall(s => List(EntryStatus.Completed, EntryStatus.Killed).contains(s))
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

  def getNearlineResults(projectId: Int): Future[Either[String, Seq[OnlineOutputMessage]]] = {
    matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
      searchAssociatedNearlineMedia(projectId, vault).map(Right.apply)
    }
  }

  def getOnlineResults(projectId: Int): Future[Right[Nothing, Seq[OnlineOutputMessage]]] = {
    searchAssociatedOnlineMedia(vidispineCommunicator, projectId).map(Right.apply)
  }

  def getOnlineResults3(projectId: Int): Future[Right[Nothing, Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]]] = {
    val future: Future[Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]] = searchAssociatedOnlineMedia3(vidispineCommunicator, projectId)
    val future1: Future[Right[Nothing, Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]]] = future.map(Right.apply)
    future1
  }

  def filterNoNoNo(nearlineResults: Seq[OnlineOutputMessage], no: Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]) = {
    val verboten = no.map(_._2.nearlineId).collect({case Some(x) => x})
    nearlineResults.filterNot(n => verboten.contains(n.nearlineId.get))
  }

  def handleUpdateMessage3(updateMessage: ProjectUpdateMessage, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    updateMessage.status match {
      case status if statusesMediaNotRequired.contains(status) =>
        //        val onlineResultsFut: Future[Right[Nothing, Seq[(PlutoCoreMessageProcessor.SendRemoveAction.Value, OnlineOutputMessage)]]] = getOnlineResults3(updateMessage.id)
        val onlineResultsFut = searchAssociatedOnlineMedia3(vidispineCommunicator, updateMessage.id)

        onlineResultsFut.map(onlineResults => {
          println(s"abcd ${onlineResults.head}")

          val buckets = toBuckets(onlineResults)
          println(s"abcd bukets ${buckets}")
          val no = buckets._1
          val yes = buckets._2
          val onlineOnly = buckets._3

          val nearlineResultsEitherFut: Future[Either[String, Seq[OnlineOutputMessage]]] = matrixStoreBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault =>
            searchAssociatedNearlineMedia(updateMessage.id, vault).map(Right.apply)
          }

          nearlineResultsEitherFut.map({
            case Right(nearlineResults) =>
              println(s"value: ${nearlineResults.map(_.nearlineId.get)}")
              println(s"no: ${no.map(_._2.nearlineId.getOrElse("<no nearline ID>"))}")
              println(s"onlyOnline: ${onlineOnly.map(_._2.nearlineId.getOrElse("<no nearline ID>"))}")
              val filtered1: Seq[OnlineOutputMessage] = filterNoNoNo(nearlineResults, no)
              println(s"filtered: ${filtered1.map(_.nearlineId.get)}")

              val noIds = no.map(_._2.nearlineId).collect({ case Some(x) => x })
              val onlyOnlineIds = onlineOnly.map(_._2.nearlineId).collect({ case Some(x) => x })
              val filtered2: Seq[OnlineOutputMessage] = filtered1.filterNot(n => onlyOnlineIds.contains(n.nearlineId.get))
//              nearlineResults.filterNot(n => verboten.contains(n.nearlineId.get))
              val filtered3 = nearlineResults
                .filterNot(n => onlyOnlineIds.contains(n.nearlineId.get))
                .filterNot(n => noIds.contains(n.nearlineId.get))

              println(s"noIds: ${noIds}")
              println(s"onlyOnlineIds: ${onlyOnlineIds}")
              println(s"filtered3: ${filtered3.map(_.nearlineId.get)}")

            case Left(err) => println(err)
          })
        })

        Future.failed(SilentDropMessage(Some(s"Testing handleUpdateMessage3 (${updateMessage.status}).")))
      //        val o3 = for {
//          onlineResults <- getOnlineResults3(updateMessage.id)
////          buckets <- Future(toBuckets(onlineResults.value))
//          nearlineResults <- getNearlineResults(updateMessage.id)
//        } yield onlineResults, nea

//        o3.foreach(_ => println("Hej"))
//        println(s"o3: ${o3.map(_._2)}")
//
////        val buckets = onlineResultsFut.map(s => toBuckets(s.value))
//        o3.map(r =>  r._2.foreach(b => println(s"------------------- OMG 1! ${b._1}")))
//        o3.map(r =>  r._2.foreach(b => println(s"------------------- OMG 2! ${b._1}")))
//        o3.map(r =>  r._2.foreach(b => println(s"------------------- OMG 3! ${b._1}")))
//        buckets.foreach(b => println(s"OMG 2! ${b._2}"))
//        buckets.foreach(b => println(s"OMG 3! ${b._3}"))

      //        Future
//          .sequence(Seq(getNearlineResults(updateMessage.id), getOnlineResults(updateMessage.id)))
//          .map(allResults => processResults(allResults, RoutingKeys.MediaNotRequired, framework, updateMessage.id, updateMessage.status))
      case _ => Future.failed(SilentDropMessage(Some(s"Incoming project update message has a status we don't care about (${updateMessage.status}), dropping it.")))
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

object PlutoCoreMessageProcessor {
  object SendRemoveAction extends Enumeration {
    val No, OnlyOnline, Yes = Value
  }
}
