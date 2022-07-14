import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.{AssetSweeperNewFile, MultiProjectOnlineOutputMessage}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.EntryStatus
import messages.MediaRemovedMessage

import scala.language.postfixOps
//import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
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

  "MediaNotRequiredMessageProcessor.handleMessage" should {
//    "route online" in {
//      val mockMsgFramework = mock[MessageProcessingFramework]
//      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
//      nearlineRecordDAO.writeRecord(any) returns Future(123)
//      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
//      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
//      failureRecordDAO.writeRecord(any) returns Future(234)
//      implicit val vidispineCommunicator = mock[VidispineCommunicator]
//
//
//      implicit val mat:Materializer = mock[Materializer]
//      implicit val sys:ActorSystem = mock[ActorSystem]
//      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//      implicit val fileCopier = mock[FileCopier]
//      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
//      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
//      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)
//
//      val fakeProject = mock[ProjectRecord]
//      fakeProject.status returns EntryStatus.New
//      fakeProject.deep_archive returns Some(true)
//      fakeProject.deletable returns None
//      fakeProject.sensitive returns None
//      val mockAssetFolderLookup = mock[AssetFolderLookup]
//      mockAssetFolderLookup.getProjectMetadata(any) returns Future(Some(fakeProject))
//
//      val mockVault = mock[Vault]
//      val mockObject = mock[MxsObject]
//      mockVault.getObject(any) returns mockObject
//
//      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
////        override protected lazy val asLookup: AssetFolderLookup = mockAssetFolderLookup
//      }
//
//      val msgContent =
//        """{
//          |"mediaTier": "ONLINE",
//          |"projectId": 23,
//          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
//          |"itemId": "VX-151949",
//          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
//          |"mediaCategory": "Rushes"
//          |}""".stripMargin
//
//      val msg = io.circe.parser.parse(msgContent)
//      val result = Try {
//        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.online", msg.right.get, mockMsgFramework), 2.seconds)
//      }
//
//      result must beASuccessfulTry
//      result.get must beLeft
//      result.get.left.get mustEqual "testing online"
//    }

//    "route nearline" in {
//      val mockMsgFramework = mock[MessageProcessingFramework]
//      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
//      nearlineRecordDAO.writeRecord(any) returns Future(123)
//      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
//      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
//      failureRecordDAO.writeRecord(any) returns Future(234)
//      implicit val vidispineCommunicator = mock[VidispineCommunicator]
//
//      implicit val mat:Materializer = mock[Materializer]
//      implicit val sys:ActorSystem = mock[ActorSystem]
//      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//      implicit val fileCopier = mock[FileCopier]
//      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
//      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
//      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)
//
//      val mockAssetFolderLookup = mock[AssetFolderLookup]
//
//      val fakeProjectDeletable = mock[ProjectRecord]
//      fakeProjectDeletable.id returns Some(23)
//      fakeProjectDeletable.status returns EntryStatus.InProduction
//      fakeProjectDeletable.deep_archive returns Some(true)
//      fakeProjectDeletable.deletable returns Some(true)
//      fakeProjectDeletable.sensitive returns None
//      mockAssetFolderLookup.getProjectMetadata("23") returns Future(Some(fakeProjectDeletable))
//
//
//      val fakeProject24 = mock[ProjectRecord]
//      fakeProject24.id returns Some(24)
//      fakeProject24.status returns EntryStatus.New
//      fakeProject24.deep_archive returns Some(true)
//      fakeProject24.deletable returns None
//      fakeProject24.sensitive returns None
//      mockAssetFolderLookup.getProjectMetadata("24") returns Future(Some(fakeProject24))
//
//      val fakeProject = mock[ProjectRecord]
//      fakeProject.id returns Some(25)
//      fakeProject.status returns EntryStatus.InProduction
//      fakeProject.deep_archive returns Some(true)
//      fakeProject.deletable returns None
//      fakeProject.sensitive returns None
//      mockAssetFolderLookup.getProjectMetadata("25") returns Future(Some(fakeProject))
//
//      val mockVault = mock[Vault]
//      val mockObject = mock[MxsObject]
//      mockVault.getObject(any) returns mockObject
//
//      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)
//
//      val msgContent =
//        """{
//          |"mediaTier": "NEARLINE",
//          |"projectIds": [24],
//          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
//          |"itemId": "VX-151949",
//          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
//          |"mediaCategory": "Rushes"
//          |}""".stripMargin
//
//      val msg = io.circe.parser.parse(msgContent)
//      val result = Try {
//        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
//      }
//
//      result must beAFailedTry(SilentDropMessage(Some("not removing file Some(VX-151949), project 24 has status In Production")))
//    }

    "22 route nearline deletable Completed project with deliverable media should drop silently " in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]

      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))

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
          |"projectIds": [22],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151922",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)
      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing file Some(VX-151922), project 22 has status Completed and is deletable, BUT the media is a deliverable"
    }


    "23 route nearline deletable Killed project with deliverable media should drop silently " in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": [23],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151923",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)
      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing file Some(VX-151923), project 23 has status Killed and is deletable, BUT the media is a deliverable"
    }


    "24 route nearline Deletable & Killed project with media not of type Deliverables should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject



      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": [24],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151924",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[MultiProjectOnlineOutputMessage]).right.get

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.filePath, msgObj.itemId, msgObj.nearlineId)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def _removeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right("pending rec removed")
        override def _deleteFromNearline(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(fakeMediaRemovedMessage)
      }

      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
      }

      println(s"24-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.itemId must beSome("VX-151924")
    }


    "26 route nearline Deletable & Completed project with media not of type Deliverables should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject


      val msgContentNotDeliverables =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": [26],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151926",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msgNotDeliverables = io.circe.parser.parse(msgContentNotDeliverables)

      val msgObj = msgNotDeliverables.flatMap(_.as[MultiProjectOnlineOutputMessage]).right.get

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.filePath, msgObj.itemId, msgObj.nearlineId)

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def _removeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right("pending rec removed")
        override def _deleteFromNearline(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(fakeMediaRemovedMessage)
      }

      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msgNotDeliverables.right.get, mockMsgFramework), 2.seconds)
      }

      println(s"26-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.itemId must beSome("VX-151926")
    }


    "27 route nearline Deletable & New project should silent drop" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val msgContentNotDeliverables =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": [27],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151927",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msgNotDeliverables = io.circe.parser.parse(msgContentNotDeliverables)
      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msgNotDeliverables.right.get, mockMsgFramework), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing file Some(VX-151927), project 27 is deletable, BUT is status is neither Completed nor Killed; it is New"
    }


    "28 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Exists on Deep Archive should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
          |"projectIds": [28],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151928",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[MultiProjectOnlineOutputMessage]).right.get


      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.filePath, msgObj.itemId, msgObj.nearlineId)


      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def _existsInDeepArchive(onlineOutputMessage: MultiProjectOnlineOutputMessage) = true
        override def _removeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right("pending rec removed")
        override def _deleteFromNearline(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(fakeMediaRemovedMessage)
        override def _storeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(true)
        override def _outputDeepArchiveCopyRequried(onlineOutputMessage: MultiProjectOnlineOutputMessage)= ???
      }


      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
      }


      println(s"28-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.itemId must beSome("VX-151928")
    }


    "29 route nearline p:deep_archive and NOT p:sensitive & p:Killed & m:Does not exist on Deep Archive should Store pending & Request copy" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
          |"projectIds": [29],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151929",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[MultiProjectOnlineOutputMessage]).right.get

      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.filePath, msgObj.itemId, msgObj.nearlineId)

      val fakeNearlineRecord = NearlineRecord.apply("aNearlineId-29", "a/path/29", "aCorrId-29")


      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def _existsInDeepArchive(onlineOutputMessage: MultiProjectOnlineOutputMessage) = false
        override def _removeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right("pending rec removed")
        override def _deleteFromNearline(onlineOutputMessage: MultiProjectOnlineOutputMessage) = ??? //Right(fakeMediaRemovedMessage)
        override def _storeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(true)
        override def _outputDeepArchiveCopyRequried(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(fakeNearlineRecord)
      }

      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
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
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
          |"projectIds": [30],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151930",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)

      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "not removing nearline media 8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056, project 30 is deep_archive and not sensitive, BUT is status is neither Completed nor Killed; it is New"
    }


    "31 route nearline p:deep_archive and p:sensitive & p:Killed & m:Exists on Internal Archive should remove media" in {
      val mockMsgFramework = mock[MessageProcessingFramework]
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      implicit val uploader = mock[FileUploader]
      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
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
          |"projectIds": [31],
          |"filePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"itemId": "VX-151931",
          |"nearlineId": "8abdd9c8-dc1e-11ec-a895-8e29f591bdb6-1056",
          |"mediaCategory": "Rushes"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)

      val msgObj = msg.flatMap(_.as[MultiProjectOnlineOutputMessage]).right.get


      val fakeMediaRemovedMessage = MediaRemovedMessage(msgObj.mediaTier, msgObj.filePath, msgObj.itemId, msgObj.nearlineId)


      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
        override def _existsInDeepArchive(onlineOutputMessage: MultiProjectOnlineOutputMessage) = false
        override def _removeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right("pending rec removed")
        override def _deleteFromNearline(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(fakeMediaRemovedMessage)
        override def _storeDeletionPending(onlineOutputMessage: MultiProjectOnlineOutputMessage) = Right(true)
        override def _outputDeepArchiveCopyRequried(onlineOutputMessage: MultiProjectOnlineOutputMessage)= ???
        override def _existsInInternalArchive(onlineOutputMessage: MultiProjectOnlineOutputMessage) = true
      }


      val result = Try {
        Await.result(toTest.handleMessage("storagetier.restorer.media_not_required.nearline", msg.right.get, mockMsgFramework), 2.seconds)
      }


      println(s"31-result: $result")
      result must beSuccessfulTry
      result.get must beRight
      result.get.right.get.content.as[MediaRemovedMessage].right.get.itemId must beSome("VX-151931")
    }


  }

//  "AssetSweeperMessageProcessor.processFile" should {
//    "perform an upload and record success if record doesn't already exist" in {
//      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
//      nearlineRecordDAO.writeRecord(any) returns Future(123)
//      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
//      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
//      failureRecordDAO.writeRecord(any) returns Future(234)
//      implicit val vidispineCommunicator = mock[VidispineCommunicator]
//
//      implicit val mat:Materializer = mock[Materializer]
//      implicit val sys:ActorSystem = mock[ActorSystem]
//      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//      implicit val fileCopier = mock[FileCopier]
//      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
//      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
//      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)
//
//      val fakeProject = mock[ProjectRecord]
//      fakeProject.deep_archive returns Some(true)
//      fakeProject.deletable returns None
//      fakeProject.sensitive returns None
//      val mockAssetFolderLookup = mock[AssetFolderLookup]
//      mockAssetFolderLookup.getProjectMetadata(any) returns Future(Some(fakeProject))
//
//
//      val mockVault = mock[Vault]
//      val mockObject = mock[MxsObject]
//      mockVault.getObject(any) returns mockObject
//
//      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
//        override protected def newCorrelationId: String = "E2C460D9-9BC5-4A84-866C-0380BF143579"
//        override protected def checkForPreExistingFiles(vault: Vault, file: AssetSweeperNewFile): Future[Option[NearlineRecord]] = mockCheckForPreExistingFiles(vault, file)
//      }
//      val mockFile = mock[AssetSweeperNewFile]
//      mockFile.filepath returns "/path/to/Assets/project"
//      mockFile.filename returns "original-file.mov"
//
//      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)
//
//      val rec: NearlineRecord = NearlineRecord(
//        id = Some(123),
//        objectId = "some-object-id",
//        originalFilePath = "/path/to/Assets/project/original-file.mov",
//        vidispineItemId = None,
//        vidispineVersionId = None,
//        proxyObjectId = None,
//        metadataXMLObjectId = None,
//        correlationId = "E2C460D9-9BC5-4A84-866C-0380BF143579"
//      )
//
//      result.map(value=>value) must beRight(rec.asJson)
//      there was one(mockCheckForPreExistingFiles).apply(mockVault, mockFile)
//    }
//
//    "perform an upload and record success if record with objectId doesn't exist in ObjectMatrix" in {
//      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
//      implicit val vidispineCommunicator = mock[VidispineCommunicator]
//
//      val rec: NearlineRecord = NearlineRecord(
//        id = Some(123),
//        objectId = "some-object-id",
//        originalFilePath = "/path/to/Assets/project/original-file.mov",
//        vidispineItemId = None,
//        vidispineVersionId = None,
//        proxyObjectId = None,
//        metadataXMLObjectId = None,
//        correlationId = UUID.randomUUID().toString
//      )
//      nearlineRecordDAO.writeRecord(any) returns Future(123)
//      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(rec))
//
//      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
//      failureRecordDAO.writeRecord(any) returns Future(234)
//
//      implicit val mat:Materializer = mock[Materializer]
//      implicit val sys:ActorSystem = mock[ActorSystem]
//      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//      implicit val fileCopier = mock[FileCopier]
//      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
//
//      val fakeProject = mock[ProjectRecord]
//      fakeProject.deep_archive returns Some(true)
//      fakeProject.deletable returns None
//      fakeProject.sensitive returns None
//      val mockAssetFolderLookup = mock[AssetFolderLookup]
//      mockAssetFolderLookup.getProjectMetadata(any) returns Future(Some(fakeProject))
//
//      val mockVault = mock[Vault]
//      //workaround from https://stackoverflow.com/questions/3762047/throw-checked-exceptions-from-mocks-with-mockito
//      mockVault.getObject(any) answers( (x:Any)=> throw new IOException("Invalid object, it does not exist (error 306)"))
//
//      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup)
//      val mockFile = mock[AssetSweeperNewFile]
//      mockFile.filepath returns "/path/to/Assets/project"
//      mockFile.filename returns "original-file.mov"
//
//      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)
//
//      result must beRight(rec.asJson)
//    }
//
//    "not perform an upload but record success if a matching file already exists in ObjectMatrix" in {
//      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
//      implicit val vidispineCommunicator = mock[VidispineCommunicator]
//
//      val rec: NearlineRecord = NearlineRecord(
//        id = Some(123),
//        objectId = "some-object-id",
//        originalFilePath = "/path/to/Assets/project/original-file.mov",
//        vidispineItemId = None,
//        vidispineVersionId = None,
//        proxyObjectId = None,
//        metadataXMLObjectId = None,
//        correlationId = "corrId"
//      )
//      nearlineRecordDAO.writeRecord(any) returns Future(123)
//      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
//
//      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
//      failureRecordDAO.writeRecord(any) returns Future(234)
//
//      implicit val mat:Materializer = mock[Materializer]
//      implicit val sys:ActorSystem = mock[ActorSystem]
//      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//      implicit val fileCopier = mock[FileCopier]
//      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Right("some-object-id"))
//
//      val fakeProject = mock[ProjectRecord]
//      fakeProject.deep_archive returns Some(true)
//      fakeProject.deletable returns None
//      fakeProject.sensitive returns None
//      val mockAssetFolderLookup = mock[AssetFolderLookup]
//      mockAssetFolderLookup.getProjectMetadata(any) returns Future(Some(fakeProject))
//
//      val mockVault = mock[Vault]
//      //workaround from https://stackoverflow.com/questions/3762047/throw-checked-exceptions-from-mocks-with-mockito
//      mockVault.getObject(any) answers( (x:Any)=> throw new IOException("Invalid object, it does not exist (error 306)"))
//
//      val mockCheckPreExisting = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
//      mockCheckPreExisting.apply(any,any) returns Future(Some(rec))
//      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
//        override protected def checkForPreExistingFiles(vault: Vault, file: AssetSweeperNewFile): Future[Option[NearlineRecord]] = mockCheckPreExisting(vault, file)
//      }
//      val mockFile = mock[AssetSweeperNewFile]
//      mockFile.filepath returns "/path/to/Assets/project"
//      mockFile.filename returns "original-file.mov"
//
//      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)
//
//      there was one(mockCheckPreExisting).apply(mockVault, mockFile)
//      there was no(fileCopier).copyFileToMatrixStore(any,any,any)
//      result must beRight(rec.asJson)
//    }
//
//    "return Failure if Left is returned when trying to copy file ObjectMatrix" in {
//      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
//      implicit val vidispineCommunicator = mock[VidispineCommunicator]
//
//      val rec: NearlineRecord = NearlineRecord(
//        id = Some(123),
//        objectId = "some-object-id",
//        originalFilePath = "/path/to/Assets/project/original-file.mov",
//        vidispineItemId = None,
//        vidispineVersionId = None,
//        proxyObjectId = None,
//        metadataXMLObjectId = None,
//        correlationId = UUID.randomUUID().toString
//      )
//      nearlineRecordDAO.writeRecord(any) returns Future(123)
//      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(rec))
//
//      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
//      failureRecordDAO.writeRecord(any) returns Future(234)
//
//      implicit val mat:Materializer = mock[Materializer]
//      implicit val sys:ActorSystem = mock[ActorSystem]
//      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//      implicit val fileCopier = mock[FileCopier]
//
//      val fakeProject = mock[ProjectRecord]
//      fakeProject.deep_archive returns Some(true)
//      fakeProject.deletable returns None
//      fakeProject.sensitive returns None
//      val mockAssetFolderLookup = mock[AssetFolderLookup]
//      mockAssetFolderLookup.getProjectMetadata(any) returns Future(Some(fakeProject))
//
//      val mockExc = new RuntimeException("ObjectMatrix out of office right now!!")
//      fileCopier.copyFileToMatrixStore(any, any, any) returns Future(Left(s"ObjectMatrix error: ${mockExc.getMessage}"))
//
//      val mockVault = mock[Vault]
//      mockVault.getObject(any) throws mockExc
//
//      val mockCheckPreExisting = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
//      mockCheckPreExisting.apply(any,any) returns Future(None)
//
//      val toTest = new MediaNotRequiredMessageProcessor(mockAssetFolderLookup) {
//        override protected def checkForPreExistingFiles(vault: Vault, file: AssetSweeperNewFile): Future[Option[NearlineRecord]] = mockCheckPreExisting(vault, file)
//      }
//
//      val mockFile = mock[AssetSweeperNewFile]
//      mockFile.filepath returns "/path/to/Assets/project"
//      mockFile.filename returns "original-file.mov"
//
//      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)
//      there was no(mockCheckPreExisting).apply(any,any)
//      result must beLeft("ObjectMatrix error: ObjectMatrix out of office right now!!")
//    }
//  }
}
