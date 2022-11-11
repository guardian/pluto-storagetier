
import PlutoCoreMessageProcessor.SendRemoveAction
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.mxscopy.{MXSConnectionBuilderImpl, MXSConnectionBuilderMock}
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, ProcessorConfiguration}
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, PlutoCoreConfig, ProductionOffice, ProjectRecord}
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

import java.nio.file.Paths
import java.time.ZonedDateTime
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

class PlutoCoreMessageProcessorSpec(implicit ec: ExecutionContext) extends Specification with Mockito {
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
  val fakePlutoConfig = PlutoCoreConfig("test","test",Paths.get("/path/to/assetfolders"))
  val asLookup = new AssetFolderLookup(fakePlutoConfig)
  val mockAsLookup = mock[AssetFolderLookup]

  "PlutoCoreMessageProcessor.isBranding(VSOnlineOutputMessage)" should {

    "return true if mediaCategory is variant of 'Branding': case 1 - lowercase" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path"), Some(1L), Some("VX-1"), Some("oid1"), "branding")
      val result = toTest.isBranding(item)

      result must beTrue
    }

    "return true if mediaCategory is variant of 'Branding': case 2 - uppercase" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path"), Some(1L), Some("VX-1"), Some("oid1"), "BRANDING")
      val result = toTest.isBranding(item)

      result must beTrue
    }

    "return true if mediaCategory is variant of 'Branding': case 3 - mixed" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path"), Some(1L), Some("VX-1"), Some("oid1"), "bRanDiNg")
      val result = toTest.isBranding(item)

      result must beTrue
    }

    "return true if mediaCategory is not variant of Branding': case 4 - rushes" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path"), Some(1L), Some("VX-1"), Some("oid1"), "rushes")
      val result = toTest.isBranding(item)

      result must beFalse
    }

    "return true if mediaCategory is exactly 'Branding'" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path"), Some(1L), Some("VX-1"), Some("oid1"), "Branding")
      val result = toTest.isBranding(item)

      result must beTrue
    }

    "filter out the branding/BRANDING/Branding/etc items" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val items = Seq(
        VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path/1"), Some(1L), Some("VX-1"), Some("oid1"), "Branding"),
        VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path/2"), Some(1L), Some("VX-2"), Some("oid2"), "rushes"),
        VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path/3"), Some(1L), Some("VX-3"), Some("oid3"), "branding"),
        VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path/4"), Some(1L), Some("VX-4"), Some("oid4"), "Branding"),
        VSOnlineOutputMessage("ONLINE", Seq(123), Some("a/path/5"), Some(1L), Some("VX-5"), Some("oid5"), "project"),
      )

      val result = items.filterNot(toTest.isBranding)

      result.size mustEqual 2
      result.head.itemId must beSome("VX-2")
    }
  }

  "PlutoCoreMessageProcessor.isBranding(ObjectMatrixEntry)" should {

    "return false if no GNM_TYPE" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      val result = toTest.isBranding(ObjectMatrixEntry("oid", Some(MxsMetadata.empty), None))

      result must beFalse
    }

    "return true if GNM_TYPE is not exactly 'Branding': case 1 - lowercase" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isBranding(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "branding")), None))

      result must beTrue
    }

    "return true if GNM_TYPE is not exactly 'Branding': case 2 - uppercase" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isBranding(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "BRANDING")), None))

      result must beTrue
    }

    "return true if GNM_TYPE is not exactly 'Branding': case 3 - mixed" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isBranding(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "bRanDiNg")), None))

      result must beTrue
    }

    "return false if GNM_TYPE is not exactly 'Branding': case 4 - rushes" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isBranding(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "rushes")), None))

      result must beFalse
    }

    "return true if GNM_TYPE is exactly 'Branding'" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isBranding(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Branding")), None))

      result must beTrue
    }

    "filter out the 'Branding' items" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val entries = Seq(
        ObjectMatrixEntry("oid1", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Branding")), None),
        ObjectMatrixEntry("oid2", Some(MxsMetadata.empty.withValue("GNM_TYPE", "rushes")), None),
        ObjectMatrixEntry("oid3", Some(MxsMetadata.empty.withValue("GNM_TYPE", "branding")), None),
        ObjectMatrixEntry("oid4", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Branding")), None),
        ObjectMatrixEntry("oid5", Some(MxsMetadata.empty.withValue("GNM_TYPE", "project")), None),
        ObjectMatrixEntry("oid6", Some(MxsMetadata.empty), None),
      )

      val result = entries.filterNot(toTest.isBranding)

      result.size mustEqual 3
      result.head.oid mustEqual "oid2"
    }
  }

  "PlutoCoreMessageProcessor.isMetadataOrProxy(ObjectMatrixEntry)" should {

    "return false if no GNM_TYPE" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      val result = toTest.isMetadataOrProxy(ObjectMatrixEntry("oid", Some(MxsMetadata.empty), None))

      result must beFalse
    }

    "return true if GNM_TYPE is exactly 'Proxy'" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      val result = toTest.isMetadataOrProxy(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Proxy")), None))

      result must beTrue
    }

    "return true if GNM_TYPE is exactly 'proxy'" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isMetadataOrProxy(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "proxy")), None))

      result must beTrue
    }

    "return true if GNM_TYPE is exactly 'metadata'" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.isMetadataOrProxy(ObjectMatrixEntry("oid", Some(MxsMetadata.empty.withValue("GNM_TYPE", "metadata")), None))

      result must beTrue
    }

    "filter out the 'metadata' and 'proxy' items" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val entries = Seq(
        ObjectMatrixEntry("oid1", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Branding")), None),
        ObjectMatrixEntry("oid2", Some(MxsMetadata.empty.withValue("GNM_TYPE", "rushes")), None),
        ObjectMatrixEntry("oid3", Some(MxsMetadata.empty.withValue("GNM_TYPE", "branding")), None),
        ObjectMatrixEntry("oid4", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Branding")), None),
        ObjectMatrixEntry("oid5", Some(MxsMetadata.empty.withValue("GNM_TYPE", "project")), None),
        ObjectMatrixEntry("oid5", Some(MxsMetadata.empty.withValue("GNM_TYPE", "Proxy")), None),
        ObjectMatrixEntry("oid5", Some(MxsMetadata.empty.withValue("GNM_TYPE", "proxy")), None),
        ObjectMatrixEntry("oid5", Some(MxsMetadata.empty.withValue("GNM_TYPE", "metadata")), None),
        ObjectMatrixEntry("oid6", Some(MxsMetadata.empty), None),
      )

      val result = entries.filterNot(toTest.isMetadataOrProxy)

      result.size mustEqual 6
      result.head.oid mustEqual "oid1"
    }
  }

  "PlutoCoreMessageProcessor" should {

    implicit val mockActorSystem = mock[ActorSystem]
    val vault = mock[Vault]
    implicit val mockMat = mock[Materializer]
    implicit val mockBuilder = MXSConnectionBuilderMock(vault)

    "ad" in {
      val fakeConfig = PlutoCoreConfig("test","test",Paths.get("/path/to/assetfolders"))
      val realAsLookup = new AssetFolderLookup(fakeConfig)
      val mockAsLookup = mock[AssetFolderLookup]
//      mockAsLookup.relativizeFilePath(any) answers ((args: Any) => realAsLookup.relativizeFilePath(args.asInstanceOf[Path]))
      mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(ProjectRecord(None, 1, "test", ZonedDateTime.now(), ZonedDateTime.now(), "test", None, None, None, None, None, EntryStatus.InProduction, ProductionOffice.UK)))


      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
//      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server", "notsecret", basePath), fakeDeliverablesConfig) {
//        override protected lazy val asLookup = mockASLookup
//      }

      ok
    }

    "drop message in handleMessage if wrong routing key" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      val emptyJson = Json.fromString("")

      val result = Try {
        Await.result(toTest.handleMessage("NOT.core.project.update", emptyJson, framework ), 3.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Not meant to receive this"
    }


    "drop message in handleUpdateMessage if status is In Production" in {

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      val updateMessage = ProjectUpdateMessage(
        status = EntryStatus.InProduction.toString,
        id = 233,
        projectTypeId = 2,
        title = "abcdefg",
        created = None,
        updated = None,
        user = "le user",
        workingGroupId = 100,
        commissionId = 200,
        deletable = true,
        deep_archive = false,
        sensitive = false,
        productionOffice = "LDN")

      val result = Try {
        Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Incoming project update message has a status we don't care about (In Production), dropping it."
    }

    "drop message in handleUpdateMessage if status is New" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      val updateMessage = ProjectUpdateMessage(
        status = EntryStatus.New.toString,
        id = 233,
        projectTypeId = 2,
        title = "abcdefg",
        created = None,
        updated = None,
        user = "le user",
        workingGroupId = 100,
        commissionId = 200,
        deletable = true,
        deep_archive = false,
        sensitive = false,
        productionOffice = "LDN")

      val result = Try {
        Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Incoming project update message has a status we don't care about (New), dropping it."
    }


    "return message with correct amount of associated files if status is Completed" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val onlineResults = for (i <- 1 to 3) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Completed.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

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

    "return message with correct amount of associated files if status is Held" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val onlineResults = for (i <- 1 to 3) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Held.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

      result must beRight
      val resData = result.map(_.content).getOrElse("".asJson).as[RestorerSummaryMessage]
      resData must beRight
      val summaryMessage = resData.right.get
      summaryMessage.projectId mustEqual 233
      summaryMessage.completed must beLessThanOrEqualTo(ZonedDateTime.now())
      summaryMessage.projectState mustEqual "Held"
      summaryMessage.numberOfAssociatedFilesNearline mustEqual 2
      summaryMessage.numberOfAssociatedFilesOnline mustEqual 3
    }

    "return message with correct amount of associated files if status is Killed" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val onlineResults = for (i <- 1 to 3) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Killed.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

      result must beRight
      val resData = result.map(_.content).getOrElse("".asJson).as[RestorerSummaryMessage]
      resData must beRight
      val summaryMessage = resData.right.get
      summaryMessage.projectId mustEqual 233
      summaryMessage.completed must beLessThanOrEqualTo(ZonedDateTime.now())
      summaryMessage.projectState mustEqual "Killed"
      summaryMessage.numberOfAssociatedFilesNearline mustEqual 2
      summaryMessage.numberOfAssociatedFilesOnline mustEqual 3
    }


    "fail if project has too many associated online files" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val tooManyOnlineResults = for (i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(tooManyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")
      val result = Try { Await.result(toTest.handleUpdateMessage(updateMessage, framework), 3.seconds)}
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 2, onlineResults = 10001"
    }


    "fail if project has too many associated nearline files" in {
      val tooManyNearlineResults = for(i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))
      val onlineResults = for (i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(tooManyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 2"
    }


    "fail if project has too many associated nearline and online files" in {
      val tooManyNearlineResults = for(i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))
      val tooManyOnlineResults = for (i <- 1 to 10002) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(tooManyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(tooManyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 10002"
    }


    "retry if vault could not be acquired" in {
      val onlineResults = for (i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def getNearlineResults(projectId: Int) = Future(Left("No vault for you!"))
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result =  Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

      result must beLeft
      result.left.get mustEqual "No vault for you!"
    }
  }

  "PlutoCoreMessageProcessor.onlineFilesByProject3" should {

    implicit val mockActorSystem = mock[ActorSystem]
    val vault = mock[Vault]
    implicit val mockMat = mock[Materializer]
    implicit val mockBuilder = MXSConnectionBuilderMock(vault)
    implicit val mockVidispineCommunicator = mock[VidispineCommunicator]

    def projectWithStatus(status: EntryStatus.Value) = {
      ProjectRecord(
        id = None,
        projectTypeId = 1,
        title = "test",
        created = ZonedDateTime.now(),
        updated = ZonedDateTime.now(),
        user = "test",
        workingGroupId = None,
        commissionId = None,
        deletable = None,
        deep_archive = None,
        sensitive = None,
        status = status,
        productionOffice = ProductionOffice.UK)
    }


    "onlineFilesByProject3" in {

      val asLookup = new AssetFolderLookup(fakePlutoConfig)
      val mockAsLookup = mock[AssetFolderLookup]

      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))

      val onlineResults = Seq(
        VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(100), filePath = Some(s"filePath1"), fileSize = Some(1024), itemId = Some(s"VX-1"), nearlineId = Some(s"mxsOid-11"), mediaCategory = "Rushes"),
        VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(200), filePath = Some(s"filePath2"), fileSize = Some(1024), itemId = Some(s"VX-2"), nearlineId = Some(s"mxsOid-22"), mediaCategory = "Rushes"),
        VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(250), filePath = Some(s"filePath4"), fileSize = Some(1024), itemId = Some(s"VX-4"), nearlineId = Some(s"mxsOid-44"), mediaCategory = "Rushes"),
        VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(300), filePath = Some(s"filePath3"), fileSize = Some(1024), itemId = Some(s"VX-3"), nearlineId = Some(s"mxsOid-33"), mediaCategory = "Rushes"),
        VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(301), filePath = Some(s"filePath3"), fileSize = Some(1024), itemId = Some(s"VX-3"), nearlineId = Some(s"mxsOid-34"), mediaCategory = "Branding"),
      )


      implicit val mockVidispineCommunicator = mock[VidispineCommunicator]
      mockVidispineCommunicator.getFilesOfProject(any, any) returns Future(onlineResults)

      mockAsLookup.getProjectMetadata("100") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("200") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("250") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("999") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)


      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
//        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
//        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(ProjectRecord(None, 1, "test", ZonedDateTime.now(), ZonedDateTime.now(), "test", None, None, None, None, None, EntryStatus.InProduction, ProductionOffice.UK)))
      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Completed.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.onlineFilesByProject3(mockVidispineCommunicator, 1), 2.seconds)

      result.foreach(r => println(s"${r._1}, ${r._2.vidispineItemId.get}"))
      println(s"onlineFilesByProject3 $result")
      result.size mustEqual 4
    }

    "searchAssociatedOnlineMedia3" in {

      val asLookup = new AssetFolderLookup(fakePlutoConfig)
      val mockAsLookup = mock[AssetFolderLookup]

      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))

      val onlineResults = Seq(
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath1"), fileSize = Some(1024), itemId = Some(s"VX-1"), nearlineId = Some(s"mxsOid-11"), mediaCategory = "Rushes", projectIds = Seq(8000, 10)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath2"), fileSize = Some(1024), itemId = Some(s"VX-2"), nearlineId = Some(s"mxsOid-22"), mediaCategory = "Rushes", projectIds = Seq(8000, 20, 8000)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath4"), fileSize = Some(1024), itemId = Some(s"VX-4"), nearlineId = Some(s"mxsOid-44"), mediaCategory = "Rushes", projectIds = Seq(8000, 300, 310, 320)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath3"), fileSize = Some(1024), itemId = Some(s"VX-3"), nearlineId = Some(s"mxsOid-33"), mediaCategory = "Rushes", projectIds = Seq(8000, 8000)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath3"), fileSize = Some(1024), itemId = Some(s"VX-3"), nearlineId = Some(s"mxsOid-34"), mediaCategory = "Branding", projectIds = Seq(9000)),
      )


      implicit val mockVidispineCommunicator = mock[VidispineCommunicator]
      mockVidispineCommunicator.getFilesOfProject(any, any) returns Future(onlineResults)

      mockAsLookup.getProjectMetadata("10") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("11") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("12") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("20") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("21") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("22") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("310") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("320") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("8000") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("8100") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("8200") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("9000") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("9100") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("9200") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)
      mockAsLookup.getProjectMetadata("2") returns Future(None)
      mockAsLookup.getProjectMetadata("3") returns Future(None)


      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)

      mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(ProjectRecord(None, 1, "test", ZonedDateTime.now(), ZonedDateTime.now(), "test", None, None, None, None, None, EntryStatus.InProduction, ProductionOffice.UK)))
      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Completed.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.searchAssociatedOnlineMedia3(mockVidispineCommunicator, 1), 2.seconds)

      result.foreach(r => println(s"${r._1}, ${r._2.vidispineItemId}."))
      println(s"onlineFilesByProject3 $result")
      result.size mustEqual 2
    }
    "handleUpdateMessage3" in {

      val asLookup = new AssetFolderLookup(fakePlutoConfig)
      val mockAsLookup = mock[AssetFolderLookup]

//      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))

      val nearlineResults = Seq(
        InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid-44", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"filepath4").withValue("GNM_PROJECT_ID", "44").withValue("GNM_TYPE", "rushes")), fileAttribues = None)),
        InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid-10", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"filepath0").withValue("GNM_PROJECT_ID", "10").withValue("GNM_TYPE", "rushes")), fileAttribues = None)),
        InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid-11", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"filepath1").withValue("GNM_PROJECT_ID", "11").withValue("GNM_TYPE", "rushes")), fileAttribues = None)),
      )

      val onlineResults = Seq(
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath1"), fileSize = Some(1024), itemId = Some(s"VX-1"), nearlineId = Some(s"mxsOid-11"), mediaCategory = "Rushes", projectIds = Seq(8000, 10)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath2"), fileSize = Some(1024), itemId = Some(s"VX-2"), nearlineId = Some(s"mxsOid-22"), mediaCategory = "Rushes", projectIds = Seq(8000, 20, 8000)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath4"), fileSize = Some(1024), itemId = Some(s"VX-4"), nearlineId = Some(s"mxsOid-44"), mediaCategory = "Rushes", projectIds = Seq(8000, 300, 310, 320)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath3"), fileSize = Some(1024), itemId = Some(s"VX-3"), nearlineId = Some(s"mxsOid-33"), mediaCategory = "Rushes", projectIds = Seq(8000, 8000)),
        VSOnlineOutputMessage(mediaTier = "ONLINE", filePath = Some(s"filePath3"), fileSize = Some(1024), itemId = Some(s"VX-3"), nearlineId = Some(s"mxsOid-34"), mediaCategory = "Branding", projectIds = Seq(9000)),
      )


      implicit val mockVidispineCommunicator = mock[VidispineCommunicator]
      mockVidispineCommunicator.getFilesOfProject(any, any) returns Future(onlineResults)

      mockAsLookup.getProjectMetadata("10") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("11") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("12") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("20") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("21") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("22") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("310") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("320") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("8000") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("8100") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("8200") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("9000") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("9100") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("9200") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)
      mockAsLookup.getProjectMetadata("2") returns Future(None)
      mockAsLookup.getProjectMetadata("3") returns Future(None)


      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
      }

      mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(ProjectRecord(None, 1, "test", ZonedDateTime.now(), ZonedDateTime.now(), "test", None, None, None, None, None, EntryStatus.InProduction, ProductionOffice.UK)))

      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Held.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.handleUpdateMessage3(updateMessage, framework), 2.seconds)


      println(s"handleUpdateMessage3 $result")
      ok
    }

    "return message with correct amount of associated files if status is Held" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val onlineResults = for (i <- 1 to 3) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Held.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

      result must beRight
      val resData = result.map(_.content).getOrElse("".asJson).as[RestorerSummaryMessage]
      resData must beRight
      val summaryMessage = resData.right.get
      summaryMessage.projectId mustEqual 233
      summaryMessage.completed must beLessThanOrEqualTo(ZonedDateTime.now())
      summaryMessage.projectState mustEqual "Held"
      summaryMessage.numberOfAssociatedFilesNearline mustEqual 2
      summaryMessage.numberOfAssociatedFilesOnline mustEqual 3
    }

    "return message with correct amount of associated files if status is Killed" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val onlineResults = for (i <- 1 to 3) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(status = EntryStatus.Killed.toString, id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, productionOffice = "LDN")

      val result = Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

      result must beRight
      val resData = result.map(_.content).getOrElse("".asJson).as[RestorerSummaryMessage]
      resData must beRight
      val summaryMessage = resData.right.get
      summaryMessage.projectId mustEqual 233
      summaryMessage.completed must beLessThanOrEqualTo(ZonedDateTime.now())
      summaryMessage.projectState mustEqual "Killed"
      summaryMessage.numberOfAssociatedFilesNearline mustEqual 2
      summaryMessage.numberOfAssociatedFilesOnline mustEqual 3
    }


    "fail if project has too many associated online files" in {
      val nearlineResults = for(i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(ObjectMatrixEntry(oid = s"mxsOid$i", attributes = Some(MxsMetadata.empty.withValue("MXFS_PATH", s"mxfspath/$i").withValue("GNM_PROJECT_ID", "233").withValue("GNM_TYPE", "rushes")), fileAttribues = None))
      val tooManyOnlineResults = for (i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(nearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(tooManyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")
      val result = Try { Await.result(toTest.handleUpdateMessage(updateMessage, framework), 3.seconds)}
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 2, onlineResults = 10001"
    }


    "fail if project has too many associated nearline files" in {
      val tooManyNearlineResults = for(i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))
      val onlineResults = for (i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(tooManyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 2"
    }


    "fail if project has too many associated nearline and online files" in {
      val tooManyNearlineResults = for(i <- 1 to 10001) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))
      val tooManyOnlineResults = for (i <- 1 to 10002) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def nearlineFilesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(tooManyNearlineResults)
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(tooManyOnlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result = Try { Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "Too many files attached to project 233, nearlineResults = 10001, onlineResults = 10002"
    }


    "retry if vault could not be acquired" in {
      val onlineResults = for (i <- 1 to 2) yield InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage(mediaTier = "ONLINE", projectIds = Seq(233), filePath = Some(s"filePath$i"), fileSize = Some(1024), itemId = Some(s"VX-$i"), nearlineId = Some(s"mxsOid-${i+1}"), mediaCategory = "Branding"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup) {
        override def getNearlineResults(projectId: Int) = Future(Left("No vault for you!"))
        override def onlineFilesByProject(vidispineCommunicator: VidispineCommunicator, projectId: Int): Future[Seq[OnlineOutputMessage]] = Future(onlineResults)
      }

      val updateMessage = ProjectUpdateMessage(id = 233, projectTypeId = 2, title = "abcdefg", created = None, updated = None, user = "le user", workingGroupId = 100, commissionId = 200, deletable = true, deep_archive = false, sensitive = false, status = EntryStatus.Completed.toString, productionOffice = "LDN")

      val result =  Await.result(toTest.handleUpdateMessage(updateMessage, framework), 2.seconds)

      result must beLeft
      result.left.get mustEqual "No vault for you!"
    }
  }

  "PlutoCoreMessageProcessor.isDeletableInAllProjects(VSOnlineOutputMessage)" should {

    val asLookup = new AssetFolderLookup(fakePlutoConfig)
    val mockAsLookup = mock[AssetFolderLookup]

    def projectWithStatus(status: EntryStatus.Value) = {
      ProjectRecord(
        id = None,
        projectTypeId = 1,
        title = "test",
        created = ZonedDateTime.now(),
        updated = ZonedDateTime.now(),
        user = "test",
        workingGroupId = None,
        commissionId = None,
        deletable = None,
        deep_archive = None,
        sensitive = None,
        status = status,
        productionOffice = ProductionOffice.UK)
    }

    "return 'still in use' if InProduction" in {
      mockAsLookup.getProjectMetadata("100") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("200") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("250") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("999") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage("ONLINE", Seq(200, 250, 100, 999, 300, 1), Some("a/path/123"), Some(1L), Some("VX-1"), Some("oid1"), "Rushes"))
      val result = Await.result(toTest.isDeletableInAllProjectsFut(item), 2.seconds)

      result mustEqual SendRemoveAction.No
    }

    "return 'held' if only Held and None" in {
      val asLookup = new AssetFolderLookup(fakePlutoConfig)
      val mockAsLookup = mock[AssetFolderLookup]

      mockAsLookup.getProjectMetadata("100") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("200") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("251") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("999") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("2") returns Future(None)

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage("ONLINE", Seq(251, 2), Some("a/path/123"), Some(1L), Some("VX-1"), Some("oid1"), "Rushes"))
      val result = Await.result(toTest.isDeletableInAllProjectsFut(item), 2.seconds)

      result mustEqual SendRemoveAction.OnlyOnline
    }

    "return 'free to remove' if only Completed and Killed" in {
      val asLookup = new AssetFolderLookup(fakePlutoConfig)
      val mockAsLookup = mock[AssetFolderLookup]

      mockAsLookup.getProjectMetadata("100") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("200") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("250") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("301") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("998") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage("ONLINE", Seq(301, 998), Some("a/path/123"), Some(1L), Some("VX-1"), Some("oid1"), "Rushes"))
      val result = Await.result(toTest.isDeletableInAllProjectsFut(item), 2.seconds)

      result mustEqual SendRemoveAction.Yes
    }

    "return 'free to remove' if only None" in {
      val asLookup = new AssetFolderLookup(fakePlutoConfig)
      val mockAsLookup = mock[AssetFolderLookup]
      mockAsLookup.getProjectMetadata("100") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("200") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("250") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("999") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage("ONLINE", Seq(1), Some("a/path/123"), Some(1L), Some("VX-1"), Some("oid1"), "Rushes"))
      val result = Await.result(toTest.isDeletableInAllProjectsFut(item), 2.seconds)

      result mustEqual SendRemoveAction.Yes
    }

    "return 'free to remove' if empty" in {
      mockAsLookup.getProjectMetadata("100") returns Future(Some(projectWithStatus(EntryStatus.New)))
      mockAsLookup.getProjectMetadata("200") returns Future(Some(projectWithStatus(EntryStatus.InProduction)))
      mockAsLookup.getProjectMetadata("250") returns Future(Some(projectWithStatus(EntryStatus.Held)))
      mockAsLookup.getProjectMetadata("300") returns Future(Some(projectWithStatus(EntryStatus.Completed)))
      mockAsLookup.getProjectMetadata("999") returns Future(Some(projectWithStatus(EntryStatus.Killed)))
      mockAsLookup.getProjectMetadata("1") returns Future(None)

      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val item = InternalOnlineOutputMessage.toOnlineOutputMessage(VSOnlineOutputMessage("ONLINE", Seq(), Some("a/path/123"), Some(1L), Some("VX-1"), Some("oid1"), "Rushes"))
      val result = Await.result(toTest.isDeletableInAllProjectsFut(item), 2.seconds)

      result mustEqual SendRemoveAction.Yes
    }
  }

  "PlutoCoreMessageProcessor.stillInUse" should {
    val HELD = Some(EntryStatus.Held)
    val NEW = Some(EntryStatus.New)
    val COMPLETED = Some(EntryStatus.Completed)
    val KILLED = Some(EntryStatus.Killed)
    val IN_PRODUCTION = Some(EntryStatus.InProduction)

    "return true if any New or InProduction" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.stillInUse(Seq(HELD, NEW))
      result must beTrue
    }

    "return false if no New or InProduction" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.stillInUse(Seq(HELD, KILLED, COMPLETED, HELD))
      result must beFalse
    }

    "return false if Held and None" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.stillInUse(Seq(HELD, None, HELD))
      result must beFalse
    }

    "return true if only New or InProduction" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.stillInUse(Seq(NEW, IN_PRODUCTION, NEW, IN_PRODUCTION))
      result must beTrue
    }

    "return false if no statuses" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.stillInUse(Seq(None, None))
      result must beFalse
    }

  }

  "PlutoCoreMessageProcessor.lowestIsHeld" should {
    val HELD = Some(EntryStatus.Held)
    val NEW = Some(EntryStatus.New)
    val COMPLETED = Some(EntryStatus.Completed)
    val KILLED = Some(EntryStatus.Killed)
    val IN_PRODUCTION = Some(EntryStatus.InProduction)

    "return true if Held for all" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.lowestIsHeld(Seq(HELD, HELD, HELD))
      result must beTrue
    }

    "return false if Held and Completed and Killed" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.lowestIsHeld(Seq(HELD, KILLED, COMPLETED, HELD))
      result must beFalse
    }

    "return false if Held and New and InProduction" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.lowestIsHeld(Seq(NEW, IN_PRODUCTION, HELD, IN_PRODUCTION))
      result must beFalse
    }

    "return false if no statuses" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.lowestIsHeld(Seq(None, None))
      result must beFalse
    }

  }

  "PlutoCoreMessageProcessor.releasedByAll" should {
    val HELD = Some(EntryStatus.Held)
    val NEW = Some(EntryStatus.New)
    val COMPLETED = Some(EntryStatus.Completed)
    val KILLED = Some(EntryStatus.Killed)
    val IN_PRODUCTION = Some(EntryStatus.InProduction)

    "return false if Held for all" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.releasedByAll(Seq(HELD, HELD, HELD))
      result must beFalse
    }

    "return false if Held and Completed and Killed" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.releasedByAll(Seq(HELD, KILLED, COMPLETED, HELD))
      result must beFalse
    }

    "return true if Completed and Killed" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.releasedByAll(Seq(KILLED, COMPLETED, KILLED))
      result must beTrue
    }

    "return true if no statuses" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.releasedByAll(Seq(None, None))
      result must beTrue
    }

    "return false if Held and New and InProduction" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig, mockAsLookup)
      val result = toTest.releasedByAll(Seq(NEW, IN_PRODUCTION, HELD, IN_PRODUCTION))
      result must beFalse
    }

  }

}
