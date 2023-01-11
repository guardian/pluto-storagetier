import MediaNotRequiredMessageProcessor.Action
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMessage
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, RMQDestination, SilentDropMessage}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.{OnlineOutputMessage, VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.{NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.EntryStatus
import messages.MediaRemovedMessage

import scala.language.postfixOps
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.slf4j.LoggerFactory
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

//noinspection ScalaDeprecation
class MediaNotRequiredMessageProcessorSpec extends Specification with Mockito {
  implicit val mxsConfig: MatrixStoreConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)
  private val logger = LoggerFactory.getLogger(getClass)


  "MediaNotRequiredMessageProcessor.handleDeepArchiveCompleteForOnline" should {

    "send Left if we couldn't connect to S3 for NEARLINE item" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val builder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]

      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]
      val mockProject = mock[ProjectRecord]

      val onlineOutputMessage = OnlineOutputMessage(MediaTiers.NEARLINE.toString, Seq("1"), Some("original/file/path/some.file"), Some(1024L), Some("VX-1"), Some("mxs-1"), "Rushes")

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future.failed(new RuntimeException("failed to connect"))
      }

      val result = Await.result(toTest.handleCheckDeepArchiveForNearline(mockVault, onlineOutputMessage, mockProject), 2.seconds)

      result must beLeft("Could not connect to deep archive to check if copy of NEARLINE media exists, do not delete yet. Reason: failed to connect")
    }

    "send Left if we couldn't connect to S3 for ONLINE item" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val builder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]

      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]
      val mockProject = mock[ProjectRecord]

      val onlineOutputMessage = OnlineOutputMessage(MediaTiers.ONLINE.toString, Seq("1"), Some("original/file/path/some.file"), Some(1024L), Some("VX-1"), Some("mxs-1"), "Rushes")

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future.failed(new RuntimeException("failed to connect"))
      }

      val result = Await.result(toTest.handleCheckDeepArchiveForOnline(onlineOutputMessage), 2.seconds)

      result must beLeft("Could not connect to deep archive to check if copy of NEARLINE media exists, do not delete yet. Reason: failed to connect")
    }
  }

  "MediaNotRequiredMessageProcessor.mediaExistsInDeepArchive" should {
    "relativize when called with item in path" in {
      val fakeConfig = PlutoCoreConfig("test", "test", Paths.get("/path/to/assetfolders"))

      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockVault = mock[Vault]

      val assetFolderLookup = new AssetFolderLookup(fakeConfig)

      val toTest = new MediaNotRequiredMessageProcessor(assetFolderLookup)

      val msgContent = """{"mediaTier":"NEARLINE","projectIds":["374"],"originalFilePath":"/path/to/assetfolders/Fred_In_Bed/This_Is_A_Test/david_allison_Deletion_Test_5/VX-3183.XML","fileSize":8823,"vidispineItemId":null,"nearlineId":"mxs-1","mediaCategory":"metadata"}"""

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some("aChecksum"))

      mockS3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(any, any, any) returns Future(true)

      toTest.nearlineMediaExistsInDeepArchive(mockVault, msgObj)

      there was one(mockS3ObjectChecker).nearlineMediaExistsInDeepArchive(Some("aChecksum"), 8823L, "/path/to/assetfolders/Fred_In_Bed/This_Is_A_Test/david_allison_Deletion_Test_5/VX-3183.XML", "Fred_In_Bed/This_Is_A_Test/david_allison_Deletion_Test_5/VX-3183.XML")
    }

    "strip slash when called with item not in path" in {
      val fakeConfig = PlutoCoreConfig("test", "test", Paths.get("/srv/Multimedia2/NextGenDev/Media Production/Assets/"))

      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockVault = mock[Vault]

      val assetFolderLookup = new AssetFolderLookup(fakeConfig)

      val toTest = new MediaNotRequiredMessageProcessor(assetFolderLookup)

      val msgContent = """{"mediaTier":"NEARLINE","projectIds":["374"],"originalFilePath":"/srv/Multimedia2/NextGenDev/Proxies/VX-11976.mp4","fileSize":291354,"vidispineItemId":null,"nearlineId":"741d089d-a920-11ec-a895-8e29f591bdb6-1568","mediaCategory":"proxy"}"""

      mockS3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(any, any, any) returns Future(true)

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(None)

      toTest.nearlineMediaExistsInDeepArchive(mockVault, msgObj)

      there was one(mockS3ObjectChecker).nearlineMediaExistsInDeepArchive(None, 291354L, "/srv/Multimedia2/NextGenDev/Proxies/VX-11976.mp4", "srv/Multimedia2/NextGenDev/Proxies/VX-11976.mp4")
    }
  }

  "MediaNotRequiredMessageProcessor.getActionToPerformOnline" should {
    "XYZ route online deletable Completed project with deliverable media should drop silently" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(22)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Completed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("22") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151922",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = toTest.getActionToPerformOnline(msgObj, None)

      result._1 mustEqual Action.DropMsg
    }
  }

  "MediaNotRequiredMessageProcessor.handleOnlineMediaNotRequired" should {

    "101 route online not Held, deletable but not Completed project should drop silently" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.InProduction
      fakeProject.deep_archive returns Some(true)
      fakeProject.deletable returns Some(true)
      fakeProject.sensitive returns None
      fakeProject.id returns Some(101)

      mockAssetFolderLookup.getProjectMetadata("101") returns Future(Some(fakeProject))
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["101"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_20101/monika_cvorak_MH_Investigation/Footage Vera Productions/20101-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519101",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "some-media-category"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_20101/monika_cvorak_MH_Investigation/Footage Vera Productions/20101-03-18_MH.mp4: ONLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-1519101, mediaCategory some-media-category in project 101: deletable(true), deep_archive(true), sensitive(false), status In Production"
    }

    "102 route online p:Held & m:Exists on Nearline should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Held
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns None
      fakeProject.sensitive returns None
      fakeProject.id returns Some(102)

      mockAssetFolderLookup.getProjectMetadata("102") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["102"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519102",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockOnlineHelper).deleteMediaFromOnline(any: OnlineOutputMessage)

      logger.debug(s"102-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519102")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }


    "103 route online p:Held & m:DoesNotExist on Nearline -> should Store pendingDeletion & drop message cos we rely on AssetSweeper for this" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Held
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns None
      fakeProject.sensitive returns None
      fakeProject.id returns Some(103)

      mockAssetFolderLookup.getProjectMetadata("103") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519103") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["103"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1034,
          |"vidispineItemId": "VX-1519103",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      //      val nearlineCopyRequiredMessage = VidispineMediaIngested(List(VidispineField("itemId", msgObj.vidispineItemId.get)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(false)
        //        override def outputNearlineCopyRequiredForOnline(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = Future(Right(nearlineCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"103-result: $result")
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Dropping message - wait for AssetSweeper to find and copy ONLINE Some(/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4)) to nearline vault"
    }


    "104 route online p:!Held (p:New) & p:!deletable & p:!deep_archive -> should throw illegal state exception" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.New
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(false)
      fakeProject.sensitive returns None
      fakeProject.id returns Some(104)

      mockAssetFolderLookup.getProjectMetadata("104") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["104"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1044,
          |"vidispineItemId": "VX-1519104",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was no(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockPendingDeletionRecordDAO).writeRecord(any)
      there was no(mockVidispineCommunicator).deleteItem(any)

      logger.debug(s"104-result: $result")
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Project state for removing files from project 104 is not valid, deep_archive flag is not true!"
    }

    "1041 route online p:!Held (p:Killed) & p:!deletable & p:!deep_archive -> should throw illegal state exception" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(false)
      fakeProject.sensitive returns None
      fakeProject.id returns Some(1041)

      mockAssetFolderLookup.getProjectMetadata("1041") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["1041"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 10414,
          |"vidispineItemId": "VX-15191041",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was no(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockPendingDeletionRecordDAO).writeRecord(any)
      there was no(mockVidispineCommunicator).deleteItem(any)

      logger.debug(s"1041-result: $result")
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Project state for removing files from project 1041 is not valid, deep_archive flag is not true!"
    }

    // New/InProduction, not deletable, deep_archive, sensitive/not sensitive
    "105 route online p:InProduction & p:!deletable & p:deep_archive & p:sensitive -> should silent drop " in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.InProduction
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(105)

      mockAssetFolderLookup.getProjectMetadata("105") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["105"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1054,
          |"vidispineItemId": "VX-1519105",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"105-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: ONLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-1519105, mediaCategory Rushes in project 105: deletable(false), deep_archive(true), sensitive(true), status In Production"
    }


    "106 route online p:New & p:!deletable & p:deep_archive & p:sensitive -> should silent drop " in { t
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.New
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(106)

      mockAssetFolderLookup.getProjectMetadata("106") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["106"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1064,
          |"vidispineItemId": "VX-1519106",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"106-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: ONLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-1519106, mediaCategory Rushes in project 106: deletable(false), deep_archive(true), sensitive(true), status New"
    }

    "107 route online p:InProduction & p:!deletable & p:deep_archive & p:!sensitive -> should silent drop " in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.InProduction
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      fakeProject.id returns Some(107)

      mockAssetFolderLookup.getProjectMetadata("107") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["107"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1074,
          |"vidispineItemId": "VX-1519107",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"107-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: ONLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-1519107, mediaCategory Rushes in project 107: deletable(false), deep_archive(true), sensitive(false), status In Production"
    }

    "108 route online p:New & p:!deletable & p:deep_archive & p:!sensitive -> should silent drop " in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.New
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      fakeProject.id returns Some(108)

      mockAssetFolderLookup.getProjectMetadata("108") returns Future(Some(fakeProject))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["108"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1084,
          |"vidispineItemId": "VX-1519108",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"108-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: ONLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-1519108, mediaCategory Rushes in project 108: deletable(false), deep_archive(true), sensitive(false), status New"
    }

    // Completed/Killed, not deletable, deep_archive, sensitive
    "109 route online p:Completed & p:!deletable & p:deep_archive & p:sensitive & m:Exists in Internal Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Completed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(109)

      mockAssetFolderLookup.getProjectMetadata("109") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519109") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["109"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519109",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      mockVault.getId returns "mockVault"
      val mockInternalVault = mock[Vault]
      mockInternalVault.getId returns "mockInternalVault"
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)

      val results = Seq(
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", msgObj.originalFilePath.get).withValue("__mxs__length", 1024L)), None),
      )

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(results)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"109-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519109")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "110 route online p:Killed & p:!deletable & p:deep_archive & p:sensitive & m:Exists in Internal Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(110)

      mockAssetFolderLookup.getProjectMetadata("110") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519110") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["110"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519110",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(true)
        // TODO also verify that it was called with the right vault
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was no(mockPendingDeletionRecordDAO).findByNearlineIdForNEARLINE(any)

      logger.debug(s"110-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519110")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "111 route online p:Completed & p:!deletable & p:deep_archive & p:sensitive & m:!Exists in Internal Archive -> Store pendingDeletion & Output Internal Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Completed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(111)

      val rec: NearlineRecord = NearlineRecord(
        id = Some(123),
        objectId = "some-object-id",
        originalFilePath = "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
        vidispineItemId = Some("VX-1519111"),
        vidispineVersionId = None,
        proxyObjectId = None,
        metadataXMLObjectId = None,
        correlationId = UUID.randomUUID().toString
      )
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(rec))

      mockAssetFolderLookup.getProjectMetadata("111") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519111") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["111"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519111",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val internalArchiveCopyRequiredMessageForOnline = new NearlineRecord(None, msgObj.nearlineId.get, msgObj.originalFilePath.get, msgObj.vidispineItemId, None, None, None, None, true, "aCorrId")

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)
      mockNearlineHelper.outputInternalArchiveCopyRequiredForOnline(any, any) returns Future(Right(internalArchiveCopyRequiredMessageForOnline.asJson))
      mockNearlineHelper.nearlineExistsInInternalArchive(any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))
      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(false)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"111-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireInternalArchive = result.get.getOrElse(null).content.as[NearlineRecord].right.get
      requireInternalArchive.vidispineItemId must beSome("VX-1519111")
    }

    "112 route online p:Killed & p:!deletable & p:deep_archive & p:sensitive & m:!Exists in Internal Archive -> Store pendingDeletion & Output Internal Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(112)

      mockAssetFolderLookup.getProjectMetadata("112") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519112") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val rec: NearlineRecord = NearlineRecord(
        id = Some(123),
        objectId = "some-object-id",
        originalFilePath = "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
        vidispineItemId = Some("VX-1519112"),
        vidispineVersionId = None,
        proxyObjectId = None,
        metadataXMLObjectId = None,
        correlationId = UUID.randomUUID().toString
      )
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(rec))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["112"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519112",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val internalArchiveCopyRequiredMessageForOnline = new NearlineRecord(None, msgObj.nearlineId.get, msgObj.originalFilePath.get, msgObj.vidispineItemId, None, None, None, None, true, "aCorrId")

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)
      mockNearlineHelper.outputInternalArchiveCopyRequiredForOnline(any, any) returns Future(Right(internalArchiveCopyRequiredMessageForOnline.asJson))
      mockNearlineHelper.nearlineExistsInInternalArchive(any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockPendingDeletionHelper.storeDeletionPending(any) returns Future(Right(4321))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(false)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"112-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val nearlineRec = result.get.getOrElse(null).content.as[NearlineRecord].right.get
      nearlineRec.vidispineItemId must beSome("VX-1519112")
    }

    // Completed/Killed, not deletable, deep_archive, not sensitive
    "113 route online p:Completed & p:!deletable & p:deep_archive & p:!sensitive & m:Exists in Deep Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      fakeProject.id returns Some(113)
      mockAssetFolderLookup.getProjectMetadata("113") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519113") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["113"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519113",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"113-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519113")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "114 route online p:Killed & p:!deletable & p:deep_archive & p:!sensitive & m:Exists in Deep Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      fakeProject.id returns Some(114)
      mockAssetFolderLookup.getProjectMetadata("114") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519114") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["114"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519114",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"114-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519114")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }


    "115 route online p:Completed & p:!deletable & p:deep_archive & p:!sensitive & m:!Exists in Deep Archive -> Store pendingDeletion & Output Deep Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Completed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      fakeProject.id returns Some(115)

      mockAssetFolderLookup.getProjectMetadata("115") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519115") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["115"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519115",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val deepCopyRequiredMessage = VidispineMediaIngested(List(VidispineField("itemId", msgObj.vidispineItemId.get)))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(false)
        override def outputDeepArchiveCopyRequiredForOnline(itemId: String, tier: String, path: Option[String]): Future[Either[String, MessageProcessorReturnValue]] = Future(Right(deepCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"115-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireDeepArchive = result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get
      requireDeepArchive.itemId must beSome("VX-1519115")
    }

    "116 route online p:Killed & p:!deletable & p:deep_archive & p:!sensitive & m:!Exists in Deep Archive -> Store pendingDeletion & Output Deep Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      fakeProject.id returns Some(116)

      mockAssetFolderLookup.getProjectMetadata("116") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519116") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["116"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519116",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(false)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"116-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireDeepArchiveMsg = result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get
      val rmq: RMQDestination = result.get.getOrElse(null).additionalDestinations.head
      logger.debug(s"116-rmq.outputExchange: ${rmq.outputExchange}")
      rmq.outputExchange mustEqual "vidispine-events"
      rmq.routingKey mustEqual "vidispine.itemneedsarchive.online"
      requireDeepArchiveMsg.itemId must beSome("VX-1519116")
    }

    // deletable, Completed/Killed, (nothing else matters)
    "117 route online p:Completed & p:deletable -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Completed
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns None
      fakeProject.sensitive returns None
      fakeProject.id returns Some(117)

      mockAssetFolderLookup.getProjectMetadata("117") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519117") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["117"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519117",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"117-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519117")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "118 route online p:Killed & p:deletable -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns None
      fakeProject.sensitive returns None
      fakeProject.id returns Some(118)

      mockAssetFolderLookup.getProjectMetadata("118") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519118") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

      val msgContent =
        """{
          |"mediaTier": "ONLINE",
          |"projectIds": ["118"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519118",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      mockNearlineHelper.verifyChecksumMatchUsingChecker(any, any, any) returns Future(Some("abd81f4f6c0c"))
      mockNearlineHelper.existsInTargetVaultWithMd5Match(any, any, any, any, any, any) returns Future(true)

      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))
      mockOnlineHelper.deleteMediaFromOnline(any: OnlineOutputMessage) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"118-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519118")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }
  }

  private def mediaRemovedMsgAsJson(msgObj: OnlineOutputMessage) = {
    MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId).asJson
  }

  "MediaNotRequiredMessageProcessor.handleNearlineMediaNotRequired" should {

    "22 route nearline deletable Completed project with deliverable media should drop silently" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(22)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Completed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("22") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151922",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: NEARLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-151922, mediaCategory Deliverables in project 22: deletable(true), deep_archive(true), sensitive(false), status Completed"
    }


    "23 route nearline deletable Killed project with deliverable media should drop silently " in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(23)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Killed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("23") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["23"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151923",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: NEARLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-151923, mediaCategory Deliverables in project 23: deletable(true), deep_archive(true), sensitive(false), status Killed"
    }

    "24 route nearline Deletable & Completed project with media not of type Deliverables should remove media" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(24)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Completed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("24") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["24"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151924",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"24-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151924")
    }

    "25 route nearline Deletable & Completed project with media not of type Deliverables should remove media" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.id returns Some(25)
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns Some(false)
      fakeProject.sensitive returns Some(false)
      mockAssetFolderLookup.getProjectMetadata("25") returns Future(Some(fakeProject))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["25"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1025,
          |"vidispineItemId": "VX-151925",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"25-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151925")
    }

    "925 route nearline NOT Deletable & Completed project with media not of type Deliverables should remove media" in {
      // Dave Completed 1
      // Deletable Archive Sensitive
      // No        Yes     No

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.id returns Some(925)
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(false)
      mockAssetFolderLookup.getProjectMetadata("925") returns Future(Some(fakeProject))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["925"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 10925,
          |"vidispineItemId": "VX-1519925",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"925-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-1519925")
    }

    "926 route nearline p:Deletable & p:Completed & NOT p:deep_archive & NOT p:sensitive with media not of type Deliverables should remove media" in {
      // Dave Completed 2
      // Deletable Archive Sensitive
      // Yes       No      No

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.id returns Some(926)
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns Some(false)
      fakeProject.sensitive returns Some(false)

      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      mockPendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(None)
      mockAssetFolderLookup.getProjectMetadata("926") returns Future(Some(fakeProject))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["926"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519926",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))
      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"926-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-1519926")
    }

    "927 route nearline p:NOT Deletable & p:Completed & p:deep_archive & p:sensitive and m:Exists on Internal Archive project with media not of type Deliverables should remove media" in {
      // Dave Completed 3
      // Deletable Archive Sensitive
      // No        Yes     Yes

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.id returns Some(927)
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      mockAssetFolderLookup.getProjectMetadata("927") returns Future(Some(fakeProject))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["927"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519927",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineExistsInInternalArchive(mockVault: Vault, mockInternalVault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"927-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-1519927")
    }

    "929 route nearline p:Deletable & p:Completed & p:NOT deep_archive & p:sensitive with media not of type Deliverables should remove media" in {
      // Dave Completed 5
      // Deletable Archive Sensitive
      // Yes       No      Yes

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.id returns Some(929)
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns Some(false)
      fakeProject.sensitive returns Some(true)

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      mockAssetFolderLookup.getProjectMetadata("929") returns Future(Some(fakeProject))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["929"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519929",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(Some(PendingDeletionRecord(Some(32123), msgObj.originalFilePath.get, msgObj.nearlineId, msgObj.vidispineItemId, MediaTiers.NEARLINE, 0)))
      pendingDeletionRecordDAO.deleteRecord(any) returns Future(1)
      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"929-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-1519929")
    }

    "930 route nearline remove media should not fail if just delete pendingDeletionRecord fails" in {

      val fakeProject = mock[ProjectRecord]
      fakeProject.status returns EntryStatus.Completed
      fakeProject.id returns Some(930)
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns Some(false)
      fakeProject.sensitive returns Some(true)

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]


      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      mockAssetFolderLookup.getProjectMetadata("930") returns Future(Some(fakeProject))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["930"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-1519930",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(Some(PendingDeletionRecord(Some(32123), msgObj.originalFilePath.get, msgObj.nearlineId, msgObj.vidispineItemId, MediaTiers.NEARLINE, 0)))
      pendingDeletionRecordDAO.deleteRecord(any) returns Future.failed(new RuntimeException("Could not delete record"))

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))
      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"930-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-1519930")
    }


    "26 route nearline Deletable & Killed project with media not of type Deliverables should remove media" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableAndKilled = mock[ProjectRecord]
      fakeProjectDeletableAndKilled.id returns Some(26)
      fakeProjectDeletableAndKilled.status returns EntryStatus.Killed
      fakeProjectDeletableAndKilled.deep_archive returns Some(true)
      fakeProjectDeletableAndKilled.deletable returns Some(true)
      fakeProjectDeletableAndKilled.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("26") returns Future(Some(fakeProjectDeletableAndKilled))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val msgContentNotDeliverables =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["26"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151926",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msgNotDeliverables = io.circe.parser.parse(msgContentNotDeliverables)

      val msgObj = msgNotDeliverables.flatMap(_.as[OnlineOutputMessage]).right.get

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"26-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151926")
    }


    "27 route nearline Deletable & New project should silent drop" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableAndKilled = mock[ProjectRecord]
      fakeProjectDeletableAndKilled.id returns Some(27)
      fakeProjectDeletableAndKilled.status returns EntryStatus.New
      fakeProjectDeletableAndKilled.deep_archive returns Some(true)
      fakeProjectDeletableAndKilled.deletable returns Some(true)
      fakeProjectDeletableAndKilled.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("27") returns Future(Some(fakeProjectDeletableAndKilled))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContentNotDeliverables =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["27"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151927",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContentNotDeliverables)
      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get
      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: NEARLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-151927, mediaCategory Rushes in project 27: deletable(true), deep_archive(true), sensitive(false), status New"
    }


    "28 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Exists on Deep Archive should remove media" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(28)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Killed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(false)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns Some(false)
      mockAssetFolderLookup.getProjectMetadata("28") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["28"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151928",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockPendingDeletionHelper.storeDeletionPending(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest: MediaNotRequiredMessageProcessor = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"28-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151928")
    }


    "29 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Does not exist on Deep Archive should Store pending & Request copy" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(29)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Killed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(false)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns Some(false)
      mockAssetFolderLookup.getProjectMetadata("29") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["29"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151929",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val fakeNearlineRecord = NearlineRecord.apply("aNearlineId-29", "a/path/29", "aCorrId-29")

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockPendingDeletionHelper.storeDeletionPending(any) returns Future(Right(1))

      val toTest: MediaNotRequiredMessageProcessor = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(false)
        override def outputDeepArchiveCopyRequiredForNearline(onlineOutputMessage: OnlineOutputMessage): Future[Right[Nothing, MessageProcessorReturnValue]] = Future(Right(fakeNearlineRecord.asJson))
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[NearlineRecord].right.get.correlationId mustEqual "aCorrId-29"
    }


    "30 route nearline p:deep_archive and NOT p:sensitive & not p:Killed and not p:Completed should silent drop" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(30)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.New
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(false)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns Some(false)
      mockAssetFolderLookup.getProjectMetadata("30") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["30"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151930",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)
      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Dropping request to remove /srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4: NEARLINE media with nearlineId 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId VX-151930, mediaCategory Rushes in project 30: deletable(false), deep_archive(true), sensitive(false), status New"
    }


    "31 route nearline p:deep_archive and p:sensitive & p:Killed & m:Exists on Internal Archive should remove media" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(31)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Killed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(false)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns Some(true)
      mockAssetFolderLookup.getProjectMetadata("31") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["31"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"fileSize": 1024,
          |"vidispineItemId": "VX-151931",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionHelper.removeDeletionPendingByMessage(any) returns Future(Right(1))
      mockPendingDeletionHelper.storeDeletionPending(any) returns Future(Right(1))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mediaRemovedMsgAsJson(msgObj)))

      val toTest: MediaNotRequiredMessageProcessor = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineExistsInInternalArchive(mockVault:Vault, mockInternalVault:Vault, onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      logger.debug(s"31-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151931")
    }
  }

}
