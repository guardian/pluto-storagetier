
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.mxscopy.{MXSConnectionBuilderImpl, MXSConnectionBuilderMock}
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, ProcessorConfiguration}
import com.gu.multimedia.storagetier.plutocore.EntryStatus
import com.gu.multimedia.storagetier.vidispine.{VSOnlineOutputMessage, VidispineCommunicator, VidispineConfig}
import com.om.mxs.client.japi.{MxsObject, Vault}
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}
import io.circe.Json
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{OnlineOutputMessage, ProjectUpdateMessage}
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
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, vsConfig)

      val emptyJson = Json.fromString("")

      val result = Try {
        Await.result(toTest.handleMessage("NOT.core.project.update", emptyJson, framework ), 3.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Not meant to receive this"
    }

    "return message with correct amount of associated files" in {
      val vsConfig = VidispineConfig("https://test-case","test","test")

      val entry = MxsMetadata.empty.withValue("MXFS_PATH", "1234").withValue("GNM_PROJECT_ID", 233).withValue("GNM_TYPE", "rushes")

      val singleNearlineResult = ObjectMatrixEntry("file1", Some(entry), None)
      val singleOnlineResult = VSOnlineOutputMessage("ONLINE", 233, Some("1234"), Some("VX-1"), "VX-2", "Branding")

      val onlineOutputForNearline = OnlineOutputMessage.apply(singleNearlineResult)
      val onlineOutputForOnline = OnlineOutputMessage.apply(singleOnlineResult)

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, vsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(Seq(onlineOutputForNearline))
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(Seq(onlineOutputForOnline))
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 3.seconds)

      result must beRight
      val resData = result.map(_.content).getOrElse("".asJson).as[RestorerSummaryMessage]
      resData must beRight
      val summaryMessage = resData.right.get
      summaryMessage.projectId mustEqual 233
      summaryMessage.completed must beLessThanOrEqualTo(ZonedDateTime.now())
      summaryMessage.projectState mustEqual "Completed"
      summaryMessage.numberOfAssociatedFilesNearline mustEqual 1
      summaryMessage.numberOfAssociatedFilesOnline mustEqual 1
    }

    "fail if project has too many associated online files" in {
      val vsConfig = VidispineConfig("https://test-case", "test", "test")

      val entry = MxsMetadata.empty
        .withValue("MXFS_PATH", "1234")
        .withValue("GNM_PROJECT_ID", 233)
        .withValue("GNM_TYPE", "rushes")

      val singleNearlineResult = ObjectMatrixEntry("file1", Some(entry), None)
      val singleOnlineResult = VSOnlineOutputMessage("ONLINE", 233, Some("1234"), Some("VX-1"), "VX-2", "Branding")

      val onlineOutputForNearline = OnlineOutputMessage.apply(singleNearlineResult)
      val manyOnlineResults = for (i <- 1 to 10001) yield OnlineOutputMessage.apply(VSOnlineOutputMessage("ONLINE", 233, Some(s"p$i"), Some(s"VX-$i"), s"VX-${i + 1}", "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, vsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(Seq(onlineOutputForNearline))
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(manyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")
      val result = Try { Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 3.seconds)}
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 1, onlineResults = 10001"
    }

    "fail if project has too many associated nearline files" in {
      val vsConfig = VidispineConfig("https://test-case","test","test")

      val entry = MxsMetadata.empty.withValue("MXFS_PATH", "1234").withValue("GNM_PROJECT_ID", 233).withValue("GNM_TYPE", "rushes")

      val singleNearlineResult = ObjectMatrixEntry("file1", Some(entry), None)
      val singleOnlineResult = VSOnlineOutputMessage("ONLINE", 233, Some("1234"), Some("VX-1"), "VX-2", "Branding")

      val manyNearlineResults = for (i <- 1 to 10001) yield OnlineOutputMessage.apply(ObjectMatrixEntry("file1", Some(entry), None))
      val onlineOutputForNearline = Seq(OnlineOutputMessage.apply(singleNearlineResult))
      val onlineOutputForOnline = Seq(OnlineOutputMessage.apply(singleOnlineResult))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, vsConfig) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(manyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineOutputForOnline)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleStatusMessage(updateMessage, "routing.key", framework), 3.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 1"
    }
  }
}
