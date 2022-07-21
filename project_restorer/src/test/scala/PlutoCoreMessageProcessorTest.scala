
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.mxscopy.{MXSConnectionBuilderImpl, MXSConnectionBuilderMock}
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, ProcessorConfiguration}
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.plutocore.EntryStatus
import com.gu.multimedia.storagetier.vidispine.{VSOnlineOutputMessage, VidispineCommunicator, VidispineConfig}
import com.om.mxs.client.japi.{MxsObject, Vault}
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import io.circe.Json
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{InternalOnlineOutputMessage, ProjectUpdateMessage}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.time.ZonedDateTime
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

class PlutoCoreMessageProcessorTest(implicit ec: ExecutionContext) extends Specification with Mockito {
  val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", Some("internal-archive-vault"))
  val vsConfig = VidispineConfig("https://test-case","test","test")

  val mockRmqChannel = mock[Channel]
  val mockRmqConnection = mock[Connection]
  mockRmqConnection.createChannel() returns mockRmqChannel
  val mockRmqFactory = mock[ConnectionFactory]
  mockRmqFactory.newConnection() returns mockRmqConnection

  val mockedMessageProcessor = mock[MessageProcessor]

  val handlers = Seq(
    ProcessorConfiguration("some-exchange","input.routing.key", "output.routing.key", mockedMessageProcessor)
  )

  val framework = new MessageProcessingFramework("input-queue",
    "output-exchg",
    "retry-exchg",
    "failed-exchg",
    "failed-q",
    handlers)(mockRmqChannel, mockRmqConnection)

  implicit val mockActorSystem = mock[ActorSystem]
  implicit val mockMat = mock[Materializer]
  implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

  val mockVault = mock[Vault]
  implicit val mockVidispineCommunicator = mock[VidispineCommunicator]
  val mockObject = mock[MxsObject]
  mockVault.getObject(any) returns mockObject

  "OwnMessageProcessor" should {

    implicit val mockActorSystem = mock[ActorSystem]
    val vault = mock[Vault]
    implicit val mockMat = mock[Materializer]
    implicit val mockBuilder = MXSConnectionBuilderMock(vault)


    "drop message in handleMessage if wrong routing key" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig)

      val emptyJson = Json.fromString("")

      val result = Try {
        Await.result(toTest.handleMessage("NOT.core.project.update", emptyJson, framework ), 3.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Not meant to receive this"
    }


    "return message with correct amount of associated files" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", 233).withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val onlineResults = for (i <- 1 to 3) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))


      val toTest = new PlutoCoreMessageProcessor(mxsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 2.seconds)

      result must beRight
      val resData = result.map(_.content).getOrElse("".asJson).as[RestorerSummaryMessage]
      resData must beRight
      val summaryMessage = resData.right.get
      summaryMessage.projectId mustEqual 233
      summaryMessage.completed must beLessThanOrEqualTo(ZonedDateTime.now())
      summaryMessage.projectState mustEqual "Completed"
      summaryMessage.numberOfAssociatedFilesNearline mustEqual 2
      summaryMessage.numberOfAssociatedFilesOnline mustEqual 3
    }


    "fail if project has too many associated online files" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", 233).withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val tooManyOnlineResults = for (i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))



      val toTest = new PlutoCoreMessageProcessor(mxsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(tooManyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")
      val result = Try { Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 3.seconds)}
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 2, onlineResults = 10001"
    }


    "fail if project has too many associated nearline files" in {
      val tooManyNearlineResults = for(i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))
      val onlineResults = for (i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(tooManyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 2"
    }


    "fail if project has too many associated nearline and online files" in {
      val tooManyNearlineResults = for(i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))
      val tooManyOnlineResults = for (i <- 1 to 10002) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(tooManyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(tooManyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 10002"
    }


    "retry if vault could not be acquired" in {
      val onlineResults = for (i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig) {
        override def getNearlineResults(projectId: Int) = Future(Left("No vault for you!"))
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result =  Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 2.seconds) 

      result must beLeft
      result.left.get mustEqual "No vault for you!"
    }
  }
}
