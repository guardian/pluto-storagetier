package helpers

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.storagetier.framework.MessageProcessingFramework
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.PendingDeletionRecordDAO
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.EntryStatus

import scala.language.postfixOps
import scala.util.Failure
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class NearlineHelperSpec extends Specification with Mockito{
  implicit val mxsConfig: MatrixStoreConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

    "NearlineHelper.dealWithAttFiles" should {
      "handle failed fromOID" in {

        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]

        val mockAssetFolderLookup = mock[AssetFolderLookup]

        val vault = mock[Vault]
        vault.getId returns "mockVault"

        val filePath = "/path/to/some/file.ext"

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override protected def callObjectMatrixEntryFromOID(vault: Vault, fileName: String): Future[ObjectMatrixEntry] = Future.fromTry(Failure(new RuntimeException("no oid for you")))
        }

        val result = Await.result(toTest.dealWithAttFiles(vault, "556593b10503", filePath), 2.seconds)

        result must beLeft("Something went wrong while trying to get metadata to delete ATT files for 556593b10503: java.lang.RuntimeException: no oid for you")
      }
    }

    "MediaNotRequiredMessageProcessor.existsInTargetVaultWithMd5Match" should {
      "log found object nicely" in {

        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        nearlineRecordDAO.writeRecord(any) returns Future(123)
        nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
        implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
        failureRecordDAO.writeRecord(any) returns Future(234)
        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]

        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]
        implicit val mockOnlineHelper: OnlineHelper = mock[helpers.OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[helpers.NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[helpers.PendingDeletionHelper]

        val mockAssetFolderLookup = mock[AssetFolderLookup]

        val vault = mock[Vault]
        vault.getId returns "mockVault"

        val nearlineId = "FD1F70B2A34E"
        val foundOid = "abd81f4f6c0c"
        mockChecksumChecker.verifyChecksumMatch(any(), any(), any()) returns Future(Some(foundOid))

        val filePath = "/path/to/some/file.ext"
        val results = Seq(
          ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
          ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
          ObjectMatrixEntry(foundOid, Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
        )

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
            override protected def callFindByFilenameNew(vault:Vault, fileName:String): Future[Seq[ObjectMatrixEntry]] = Future(results)
        }

        val result = Await.result(toTest.existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, vault, filePath, 12L, Some("ff961dc5e8da688fa78540651160b223")), 2.seconds)

        result must beTrue
      }

      "log no matching md5 nicely" in {

        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]

        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper: OnlineHelper = mock[helpers.OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[helpers.NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[helpers.PendingDeletionHelper]

        val mockAssetFolderLookup = mock[AssetFolderLookup]

        val vault = mock[Vault]
        vault.getId returns "mockVault"

        mockChecksumChecker.verifyChecksumMatch(any(), any(), any()) returns Future(None)

        val nearlineId = "FD1F70B2A34E"
        val filePath = "/path/to/some/file.ext"
        val results = Seq(
          ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
          ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
          ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
        )

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
            override protected def callFindByFilenameNew(vault:Vault, fileName:String): Future[Seq[ObjectMatrixEntry]] = Future(results)
        }

        val result = Await.result(toTest.existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, vault, filePath, 12L, Some("ff961dc5e8da688fa78540651160b223")), 2.seconds)

        result must beFalse
      }

      "log no matching files nicely" in {

        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        nearlineRecordDAO.writeRecord(any) returns Future(123)
        nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
        implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
        failureRecordDAO.writeRecord(any) returns Future(234)
        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]

        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper: OnlineHelper = mock[helpers.OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[helpers.NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[helpers.PendingDeletionHelper]

        val mockAssetFolderLookup = mock[AssetFolderLookup]

        val vault = mock[Vault]
        vault.getId returns "mockVault"

        mockChecksumChecker.verifyChecksumMatch(any(), any(), any()) returns Future(None)

        val nearlineId = "FD1F70B2A34E"
        val filePath = "/path/to/some/file.ext"

        val results = Seq()

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
            override protected def callFindByFilenameNew(vault:Vault, fileName:String): Future[Seq[ObjectMatrixEntry]] = Future(results)
        }

        val result = Await.result(toTest.existsInTargetVaultWithMd5Match(MediaTiers.NEARLINE, nearlineId, vault, filePath, 12L, Some("ff961dc5e8da688fa78540651160b223")), 2.seconds)

        result must beFalse
      }
    }

    "NearlineHelper.getChecksumForNearline" should {
      "fail if no nearline id" in {
        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]

        implicit val mat:Materializer = mock[Materializer]
        implicit val sys:ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper: OnlineHelper = mock[helpers.OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[helpers.NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[helpers.PendingDeletionHelper]

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

        val toTest = new NearlineHelper(mockAssetFolderLookup)

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

    "NearlineHelper.deleteMediaFromNearline" should {
      "fail if no nearline id" in {
        implicit val pendingDeletionRecordDAO :PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]

        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
        implicit val mat:Materializer = mock[Materializer]
        implicit val sys:ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
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

        val toTest = new NearlineHelper(mockAssetFolderLookup)

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

    "MediaNotRequiredMessageProcessor.nearlineExistsInInternalArchive" should {
      "unrelativize when called with item with relative path" in {
        val basePath = "/srv/Multimedia2/NextGenDev/Media Production/Assets/"
        val fakeConfig = PlutoCoreConfig("test", "test", Paths.get(basePath))

        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper: OnlineHelper = mock[helpers.OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[helpers.NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[helpers.PendingDeletionHelper]

        val assetFolderLookup = new AssetFolderLookup(fakeConfig)


        val toTest = new NearlineHelper(assetFolderLookup)

        val filePath = "Fred_In_Bed/This_Is_A_Test/david_allison_Deletion_Test_5/VX-3183.XML"
        val result = toTest.putItBack(filePath)

        result mustEqual basePath + filePath
      }

      "not unrelativize when called with item with absolute path" in {
        val basePath = "/srv/Multimedia2/NextGenDev/Media Production/Assets/"
        val fakeConfig = PlutoCoreConfig("test", "test", Paths.get(basePath))

        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
        implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper: OnlineHelper = mock[helpers.OnlineHelper]
        implicit val mockNearlineHelper: NearlineHelper = mock[helpers.NearlineHelper]
        implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[helpers.PendingDeletionHelper]

        val assetFolderLookup = new AssetFolderLookup(fakeConfig)

        val toTest = new NearlineHelper(assetFolderLookup)

        val filePath = "/srv/Multimedia2/NextGenDev/Proxies/VX-12074.mp4"
        val result = toTest.putItBack(filePath)

        result mustNotEqual basePath + filePath
        result mustEqual filePath
      }
    }

  "NearlineHelper.findMatchingFilesOnVault" should {
    "log in findMatchingFilesOnVault nicely" in {

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockChecksumChecker: ChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val vault = mock[Vault]
      vault.getId returns "mockVault"

      val filePath = "/path/to/some/file.ext"
      val results = Seq(
        ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 30L)), None),
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
        ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 20L)), None),
      )

      val toTest = new NearlineHelper(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(results)
      }

      val result = Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 10L), 2.seconds)

      result.size mustEqual 1
      result.head.oid mustEqual "abd81f4f6c0c"
    }
  }

}
