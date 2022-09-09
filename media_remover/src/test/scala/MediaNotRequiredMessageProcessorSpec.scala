import MediaNotRequiredMessageProcessor.Action
import akka.actor.ActorSystem
import akka.http.javadsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model.HttpMessage
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.{AssetSweeperNewFile, OnlineOutputMessage, VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
import com.gu.multimedia.storagetier.plutocore.EntryStatus
import messages.MediaRemovedMessage

import scala.language.postfixOps
import scala.util.{Failure, Success}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.IOException
import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class MediaNotRequiredMessageProcessorSpec extends Specification with Mockito {
  implicit val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

  "MediaNotRequiredMessageProcessor.getChecksumForNearlineItem" should {
    "fail if no nearline id" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]

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
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.getChecksumForNearline(mockVault, msgObj.nearlineId.get), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[NoSuchElementException]
      result.failed.get.getMessage mustEqual "None.get"
    }
  }

  "MediaNotRequiredMessageProcessor.deleteFromNearline" should {
    "fail if no nearline id" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]

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

//          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765",
      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.deleteMediaFromNearline(mockVault, msgObj.mediaTier, msgObj.originalFilePath, msgObj.nearlineId, msgObj.vidispineItemId), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "Cannot delete from nearline, wrong media tier (NEARLINE), or missing nearline id (None)"
    }
  }


  "MediaNotRequiredMessageProcessor.storeDeletionPending" should {
    "fail when size is None" in {

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        toTest.validateNeededFields(None, Some("path"), Some("id"))
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "fileSize is missing"
    }

    "fail when size is -1" in {

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        toTest.validateNeededFields(Some(-1), Some("path"), Some("id"))
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "fileSize is -1"
    }

    "fail when path is missing" in {

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        toTest.validateNeededFields(Some(2048), None, Some("id"))
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "filePath is missing"
    }

    "fail when id is missing" in {

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        toTest.validateNeededFields(Some(2048), Some("path"), None)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "media id is missing"
    }

    "fail when when size, path and id are all missing" in {

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        toTest.validateNeededFields(None, None, None)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "fileSize is missing"
    }

    "succeed when size, path and id are present" in {

      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        toTest.validateNeededFields(Some(2048), Some("path"), Some("id"))
      }

      result must beSuccessfulTry
    }
  }


  "MediaNotRequiredMessageProcessor.storeDeletionPending" should {
    "fail if no filePath" in {

      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      pendingDeletionRecordDAO.writeRecord(any) returns Future(234)

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"nearlineId": "1",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      println(s"sdp-a-result: $result")
      result.failed.get.getMessage mustEqual "Cannot store PendingDeletion record for item without filepath"
    }


    "store new record for NEARLINE if no record found" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(None)
      pendingDeletionRecordDAO.writeRecord(any) returns Future(234)

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"nearlineId": "1",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      result must beSuccessfulTry
      result.get must beRight(234)
    }


    "store updated record for NEARLINE if record already present" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      val existingRecord = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsid"), MediaTiers.NEARLINE, 1)
      val expectedUpdatedRecordToSave = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsid"), MediaTiers.NEARLINE, 2)
      pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(Some(existingRecord))
      pendingDeletionRecordDAO.writeRecord(expectedUpdatedRecordToSave) returns Future(234)

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"nearlineId": "1",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      result must beSuccessfulTry
      result.get must beRight(234)
    }
  }

  "MediaNotRequiredMessageProcessor.getActionToPerformOnline" should {
    "XYZ route online deletable Completed project with deliverable media should drop silently" in {
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]

      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProjectDeletableCompletedAndDeliverable = mock[ProjectRecord]
      fakeProjectDeletableCompletedAndDeliverable.id returns Some(22)
      fakeProjectDeletableCompletedAndDeliverable.status returns EntryStatus.Completed
      fakeProjectDeletableCompletedAndDeliverable.deep_archive returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.deletable returns Some(true)
      fakeProjectDeletableCompletedAndDeliverable.sensitive returns None
      mockAssetFolderLookup.getProjectMetadata("22") returns Future(Some(fakeProjectDeletableCompletedAndDeliverable))
      val someProject = Some(fakeProjectDeletableCompletedAndDeliverable)

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
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

//      val result = Try {
//      }

        val result = toTest.getActionToPerformOnline(msgObj, None)

      result._1 mustEqual Action.DropMsg
//      result must beAFailedTry
//      result.failed.get must beAnInstanceOf[SilentDropMessage]
//      result.failed.get.getMessage mustEqual "not removing nearline media 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, project 22 is deletable(true), deep_archive(true), sensitive(false), status is Completed, media category is Deliverables"

    }
  }

  "MediaNotRequiredMessageProcessor.handleOnlineMediaNotRequired" should {

    "101 route online not Held, deletable but not Completed project should drop silently" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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
      result.failed.get.getMessage mustEqual "Not removing ONLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-1519101: project (id 101) is deletable(true), deep_archive(true), sensitive(false), status is In Production, media category is some-media-category."
    }

    "102 route online p:Held & m:Exists on Nearline should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Held
      fakeProject.deletable returns Some(true)
      fakeProject.deep_archive returns None
      fakeProject.sensitive returns None
      fakeProject.id returns Some(102)

      mockAssetFolderLookup.getProjectMetadata("102") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519102") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

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

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockVidispineCommunicator).deleteItem(any)

      println(s"102-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519102")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }


    "103 route online p:Held & m:DoesNotExist on Nearline -> should Store pendingDeletion & Output Nearline copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      val nearlineCopyRequiredMessage = VidispineMediaIngested(List(VidispineField("itemId", msgObj.vidispineItemId.get)))

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(false)
        override def NOT_IMPL_outputNearlineCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = Future(Right(nearlineCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockVidispineCommunicator).deleteItem(any)
      there was one(mockPendingDeletionRecordDAO).writeRecord(any)

      println(s"103-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get.itemId must beSome("VX-1519103")
    }


    "104 route online p:!Held (p:New) & p:!deletable & p:!deep_archive -> should throw illegal state exception" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      println(s"104-result: $result")
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Project state for removing files from project 104 is not valid, deep_archive flag is not true!"
    }

    "1041 route online p:!Held (p:Killed) & p:!deletable & p:!deep_archive -> should throw illegal state exception" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      println(s"1041-result: $result")
      result must beFailedTry
      result.failed.get.getMessage mustEqual "Project state for removing files from project 1041 is not valid, deep_archive flag is not true!"
    }

    // New/InProduction, not deletable, deep_archive, sensitive/not sensitive
    "105 route online p:InProduction & p:!deletable & p:deep_archive & p:sensitive -> should silent drop " in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      println(s"105-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Not removing ONLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-1519105: project (id 105) is deletable(false), deep_archive(true), sensitive(true), status is In Production, media category is Rushes."
    }


    "106 route online p:New & p:!deletable & p:deep_archive & p:sensitive -> should silent drop " in { t
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      println(s"106-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Not removing ONLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-1519106: project (id 106) is deletable(false), deep_archive(true), sensitive(true), status is New, media category is Rushes."
    }

    "107 route online p:InProduction & p:!deletable & p:deep_archive & p:!sensitive -> should silent drop " in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      println(s"107-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Not removing ONLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-1519107: project (id 107) is deletable(false), deep_archive(true), sensitive(false), status is In Production, media category is Rushes."
    }

    "108 route online p:New & p:!deletable & p:deep_archive & p:!sensitive -> should silent drop " in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      println(s"108-result: $result")
      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "Not removing ONLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-1519108: project (id 108) is deletable(false), deep_archive(true), sensitive(false), status is New, media category is Rushes."
    }

    // Completed/Killed, not deletable, deep_archive, sensitive
    "109 route online p:Completed & p:!deletable & p:deep_archive & p:sensitive & m:Exists in Internal Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockPendingDeletionRecordDAO).findByNearlineIdForNEARLINE(any)

      there was one(mockVidispineCommunicator).deleteItem(any)

      println(s"109-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519109")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "110 route online p:Killed & p:!deletable & p:deep_archive & p:sensitive & m:Exists in Internal Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(true)
        // TODO also verify that it was called with the right vault
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockPendingDeletionRecordDAO).findByNearlineIdForNEARLINE(any)

      there was one(mockVidispineCommunicator).deleteItem(any)

      println(s"110-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519110")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "111 route online p:Completed & p:!deletable & p:deep_archive & p:sensitive & m:!Exists in Internal Archive -> Store pendingDeletion & Output Internal Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Completed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(111)

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

      val internalArchiveCopyRequiredMessage = VidispineMediaIngested(List(VidispineField("itemId", msgObj.vidispineItemId.get)))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(false)
        override def NOT_IMPL_outputInternalArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = Future(Right(internalArchiveCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockPendingDeletionRecordDAO).writeRecord(any)

      println(s"111-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireDeepArchive = result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get
      requireDeepArchive.itemId must beSome("VX-1519111")


    }

    "112 route online p:Killed & p:!deletable & p:deep_archive & p:sensitive & m:!Exists in Internal Archive -> Store pendingDeletion & Output Internal Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val fakeProject = mock[ProjectRecord]

      fakeProject.status returns EntryStatus.Killed
      fakeProject.deletable returns Some(false)
      fakeProject.deep_archive returns Some(true)
      fakeProject.sensitive returns Some(true)
      fakeProject.id returns Some(112)

      mockAssetFolderLookup.getProjectMetadata("112") returns Future(Some(fakeProject))

      mockVidispineCommunicator.deleteItem("VX-1519112") returns Future(Some(HttpMessage.AlreadyDiscardedEntity))

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

      val internalArchiveCopyRequiredMessage = VidispineMediaIngested(List(VidispineField("itemId", msgObj.vidispineItemId.get)))

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineExistsInVault(nearlineVaultOrInternalArchiveVault: Vault, vsItemId: String, filePath: String, fileSize: Long): Future[Boolean] = Future(false)
        override def NOT_IMPL_outputInternalArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Either[String, MessageProcessorReturnValue]] = Future(Right(internalArchiveCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockPendingDeletionRecordDAO).writeRecord(any)

      println(s"112-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireDeepArchive = result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get
      requireDeepArchive.itemId must beSome("VX-1519112")
    }

    // Completed/Killed, not deletable, deep_archive, not sensitive
    "113 route online p:Completed & p:!deletable & p:deep_archive & p:!sensitive & m:Exists in Deep Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockPendingDeletionRecordDAO).findByNearlineIdForNEARLINE(any)
      there was no(mockPendingDeletionRecordDAO).deleteRecord(any)

      println(s"113-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519113")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "114 route online p:Killed & p:!deletable & p:deep_archive & p:!sensitive & m:Exists in Deep Archive -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage) = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was no(mockPendingDeletionRecordDAO).findByNearlineIdForNEARLINE(any)
      there was no(mockPendingDeletionRecordDAO).deleteRecord(any)

      println(s"114-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519114")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }


    "115 route online p:Completed & p:!deletable & p:deep_archive & p:!sensitive & m:!Exists in Deep Archive -> Store pendingDeletion & Output Deep Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage) = Future(false)
        override def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage) = Future(Right(deepCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockPendingDeletionRecordDAO).writeRecord(any)

      println(s"115-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireDeepArchive = result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get
      requireDeepArchive.itemId must beSome("VX-1519115")
    }

    "116 route online p:Killed & p:!deletable & p:deep_archive & p:!sensitive & m:!Exists in Deep Archive -> Store pendingDeletion & Output Deep Archive copy required" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      //      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)
      val deepCopyRequiredMessage = VidispineMediaIngested(List(VidispineField("itemId", msgObj.vidispineItemId.get)))


      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(msgObj.vidispineItemId.get) returns Future(None)
      mockPendingDeletionRecordDAO.writeRecord(any) returns Future(4321)


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def onlineMediaExistsInDeepArchive(onlineOutputMessage: OnlineOutputMessage): Future[Boolean] = Future(false)
        override def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage): Future[Right[Nothing, MessageProcessorReturnValue]] = Future(Right(deepCopyRequiredMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockPendingDeletionRecordDAO).writeRecord(any)

      println(s"116-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val requireDeepArchive = result.get.getOrElse(null).content.as[VidispineMediaIngested].right.get
      requireDeepArchive.itemId must beSome("VX-1519116")
    }

    // deletable, Completed/Killed, (nothing else matters)
    "117 route online p:Completed & p:deletable -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockVidispineCommunicator).deleteItem(any)

      println(s"117-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519117")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }

    "118 route online p:Killed & p:deletable -> Should remove media" in {
      implicit val mockPendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockVidispineCommunicator:VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder:MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker:S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker:ChecksumChecker = mock[ChecksumChecker]
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


      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleOnlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      there was one(mockPendingDeletionRecordDAO).findByOnlineIdForONLINE(any)
      there was one(mockVidispineCommunicator).deleteItem(any)

      println(s"118-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      val mediaRemovedMessage = result.get.getOrElse(null).content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1519118")
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.ONLINE.toString
    }
  }

  "MediaNotRequiredMessageProcessor.handleNearlineMediaNotRequired" should {

    "22 route nearline deletable Completed project with deliverable media should drop silently" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]

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
      result.failed.get.getMessage mustEqual "Not removing NEARLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-151922: project (id 22) is deletable(true), deep_archive(true), sensitive(false), status is Completed, media category is Deliverables."
    }


    "23 route nearline deletable Killed project with deliverable media should drop silently " in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]

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
      result.failed.get.getMessage mustEqual "Not removing NEARLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-151923: project (id 23) is deletable(true), deep_archive(true), sensitive(false), status is Killed, media category is Deliverables."
    }


    "24 route nearline Deletable & Killed project with media not of type Deliverables should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteMediaFromNearline(mockVault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]) = Future(Right(fakeMediaRemovedMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"24-result: $result")

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151924")
    }


    "26 route nearline Deletable & Completed project with media not of type Deliverables should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteMediaFromNearline(mockVault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]) = Future(Right(fakeMediaRemovedMessage.asJson))
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"26-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151926")
    }


    "27 route nearline Deletable & New project should silent drop" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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
      result.failed.get.getMessage mustEqual "Not removing NEARLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-151927: project (id 27) is deletable(true), deep_archive(true), sensitive(false), status is New, media category is Rushes."
    }


    "28 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Exists on Deep Archive should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(true)
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteMediaFromNearline(mockVault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]) = Future(Right(fakeMediaRemovedMessage.asJson))
        override def storeDeletionPending(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage)= ???
      }


      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"28-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151928")
    }


    "29 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Does not exist on Deep Archive should Store pending & Request copy" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val fakeNearlineRecord = NearlineRecord.apply("aNearlineId-29", "a/path/29", "aCorrId-29")

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(false)
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteMediaFromNearline(mockVault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]) = ??? //Right(fakeMediaRemovedMessage)
        override def storeDeletionPending(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage) = Future(Right(fakeNearlineRecord.asJson))
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[NearlineRecord].right.get.correlationId mustEqual "aCorrId-29"
    }


    "30 route nearline p:deep_archive and NOT p:sensitive & not p:Killed and not p:Completed should silent drop" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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
      result.failed.get.getMessage mustEqual "Not removing NEARLINE media with nearlineId=8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-8765, onlineId=VX-151930: project (id 30) is deletable(false), deep_archive(true), sensitive(false), status is New, media category is Rushes."
    }


    "31 route nearline p:deep_archive and p:sensitive & p:Killed & m:Exists on Internal Archive should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
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

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val mockVault = mock[Vault]
      val mockInternalVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def nearlineMediaExistsInDeepArchive(vault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(false)
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteMediaFromNearline(mockVault: Vault, mediaTier: String, filePathMaybe: Option[String], nearlineIdMaybe: Option[String], vidispineItemIdMaybe: Option[String]) = Future(Right(fakeMediaRemovedMessage.asJson))
        override def storeDeletionPending(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage)= ???
        override def nearlineExistsInInternalArchive(mockVault:Vault, mockInternalVault:Vault, onlineOutputMessage: OnlineOutputMessage) = Future(true)
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"31-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.\\("content").head.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151931")
    }


  }

}
