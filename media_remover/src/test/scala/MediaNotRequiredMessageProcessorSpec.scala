import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.{AssetSweeperNewFile, OnlineOutputMessage}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
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
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]

      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]

      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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

//          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
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
        Await.result(toTest.deleteFromNearline(mockVault, msgObj), 2.seconds)
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
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      pendingDeletionRecordDAO.writeRecord(any) returns Future(234)

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
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
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(any, any) returns Future(None)
      pendingDeletionRecordDAO.writeRecord(any) returns Future(234)

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
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
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      val existingRecord = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsid"), MediaTiers.NEARLINE, 1)
      val expectedUpdatedRecordToSave = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsid"), MediaTiers.NEARLINE, 2)
      pendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(any, any) returns Future(Some(existingRecord))
      pendingDeletionRecordDAO.writeRecord(expectedUpdatedRecordToSave) returns Future(234)

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
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


  "MediaNotRequiredMessageProcessor.handleMessage" should {

    "22 route nearline deletable Completed project with deliverable media should drop silently" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]

      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing nearline media 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056, project 22 is deletable(true), deep_archive(true), sensitive(false), status is Completed, media category is Deliverables"
    }


    "23 route nearline deletable Killed project with deliverable media should drop silently " in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing nearline media 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056, project 23 is deletable(true), deep_archive(true), sensitive(false), status is Killed, media category is Deliverables"
    }


    "24 route nearline Deletable & Killed project with media not of type Deliverables should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteFromNearline(mockVault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(Right(fakeMediaRemovedMessage))
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"24-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151924")
    }


    "26 route nearline Deletable & Completed project with media not of type Deliverables should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msgNotDeliverables = io.circe.parser.parse(msgContentNotDeliverables)

      val msgObj = msgNotDeliverables.flatMap(_.as[OnlineOutputMessage]).right.get

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.originalFilePath.get, msgObj.vidispineItemId, msgObj.nearlineId)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def removeDeletionPendingByMessage(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def deleteFromNearline(mockVault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(Right(fakeMediaRemovedMessage))
      }

      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"26-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151926")
    }


    "27 route nearline Deletable & New project should silent drop" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContentNotDeliverables)
      val msgObj = msg.flatMap(_.as[OnlineOutputMessage]).right.get
      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing nearline media 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056, project 27 is deletable(true), deep_archive(true), sensitive(false), status is New, media category is Rushes"
    }


    "28 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Exists on Deep Archive should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
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
        override def deleteFromNearline(mockVault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(Right(fakeMediaRemovedMessage))
        override def storeDeletionPending(onlineOutputMessage: OnlineOutputMessage) = Future(Right(1))
        override def NOT_IMPL_outputDeepArchiveCopyRequired(onlineOutputMessage: OnlineOutputMessage)= ???
      }


      val result = Try {
        Await.result(toTest.handleNearlineMediaNotRequired(mockVault, mockInternalVault, msgObj), 2.seconds)
      }

      println(s"28-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151928")
    }


    "29 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Does not exist on Deep Archive should Store pending & Request copy" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
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
        override def deleteFromNearline(mockVault: Vault, onlineOutputMessage: OnlineOutputMessage) = ??? //Right(fakeMediaRemovedMessage)
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
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
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
      result.failed.get.getMessage mustEqual "not removing nearline media 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056, project 30 is deletable(false), deep_archive(true), sensitive(false), status is New, media category is Rushes"
    }


    "31 route nearline p:deep_archive and p:sensitive & p:Killed & m:Exists on Internal Archive should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

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
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
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
        override def deleteFromNearline(mockVault: Vault, onlineOutputMessage: OnlineOutputMessage) = Future(Right(fakeMediaRemovedMessage))
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
      result.get.right.get.content.as[MediaRemovedMessage].right.get.vidispineItemId must beSome("VX-151931")
    }


  }

}
