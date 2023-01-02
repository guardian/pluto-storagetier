package helpers

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.PendingDeletionRecordDAO
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.EntryStatus

import scala.language.postfixOps
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import exceptions.BailOutException
import io.circe.generic.auto._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage

import java.io.IOException
import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class NearlineHelperSpec extends Specification with Mockito{
  implicit val mxsConfig: MatrixStoreConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

  private def getOnlineOutputMessageWithMaybeNearlineId(maybeNearlineId: Option[String]) =
    OnlineOutputMessage(
      mediaTier = "NEARLINE",
      projectIds = Seq("22"),
      originalFilePath = Some("/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4"),
      fileSize = None,
      vidispineItemId = Some("VX-151922"),
      nearlineId = maybeNearlineId,
      mediaCategory = "Deliverables")


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
          override protected def callObjectMatrixEntryFromOID(vault: Vault, fileName: String): Future[ObjectMatrixEntry] = Future.failed(new RuntimeException("no oid for you"))
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
        val entries = Seq(
          ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
          ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
          ObjectMatrixEntry(foundOid, Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
        )

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
            override protected def callFindByFilenameNew(vault:Vault, fileName:String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
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
        val entries = Seq(
          ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
          ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
          ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 12L)), None),
        )

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
            override protected def callFindByFilenameNew(vault:Vault, fileName:String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
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
      "return Some checksum if found in appliance" in {
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

        val msgObj = getOnlineOutputMessageWithMaybeNearlineId(Some("mxs-22"))

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override protected def getOMFileMd5(mxsFile: MxsObject): Future[Try[String]] = Future(Try("md5-checksum-1"))
        }

        val result = Await.result(toTest.getChecksumForNearline(mockVault, msgObj.nearlineId.get), 2.seconds)

        result must beSome("md5-checksum-1")
      }

      "return None if checksum not found in appliance" in {
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

        val msgObj = getOnlineOutputMessageWithMaybeNearlineId(Some("mxs-22"))

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override protected def getOMFileMd5(mxsFile: MxsObject): Future[Try[String]] = Future.failed(new RuntimeException("Could not get valid checksum after 50 tries"))
        }

        val result = Await.result(toTest.getChecksumForNearline(mockVault, msgObj.nearlineId.get), 2.seconds)

        result must beNone
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

        val msgObj = getOnlineOutputMessageWithMaybeNearlineId(None)

        val result = Try {
          Await.result(toTest.deleteMediaFromNearline(mockVault, msgObj.mediaTier, msgObj.originalFilePath, msgObj.nearlineId, msgObj.vidispineItemId), 2.seconds)
        }

        result must beAFailedTry
        result.failed.get must beAnInstanceOf[RuntimeException]
        result.failed.get.getMessage mustEqual "Cannot delete from nearline, wrong media tier (NEARLINE), or missing nearline id (None)"
      }

      "remove media even if dealWithAttFiles fails" in {
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

        val msgObj = getOnlineOutputMessageWithMaybeNearlineId(Some("mxs-22"))

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override def dealWithAttFiles(vault: Vault, nearlineId: String, originalFilePath: String): Future[Either[String, String]] =
            Future(Left("Could not deal with ATT files"))
        }

        val result =
          Await.result(toTest.deleteMediaFromNearline(mockVault, msgObj.mediaTier, msgObj.originalFilePath, msgObj.nearlineId, msgObj.vidispineItemId), 2.seconds)

        result must beRight
        val mediaRemovedMessage = result.right.get.content.as[MediaRemovedMessage].right.get
        mediaRemovedMessage.vidispineItemId must beSome("VX-151922")
        mediaRemovedMessage.mediaTier mustEqual MediaTiers.NEARLINE.toString
      }

      "remove media if dealWithAttFiles succeeds" in {
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

        val msgObj = getOnlineOutputMessageWithMaybeNearlineId(Some("mxs-22"))

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override def dealWithAttFiles(vault: Vault, nearlineId: String, originalFilePath: String): Future[Either[String, String]] =
            Future(Right(s"The ATT files that were present for ${msgObj.nearlineId} have been deleted"))
        }

        val result =
          Await.result(toTest.deleteMediaFromNearline(mockVault, msgObj.mediaTier, msgObj.originalFilePath, msgObj.nearlineId, msgObj.vidispineItemId), 2.seconds)

        result must beRight
        val mediaRemovedMessage = result.right.get.content.as[MediaRemovedMessage].right.get
        mediaRemovedMessage.vidispineItemId must beSome("VX-151922")
        mediaRemovedMessage.mediaTier mustEqual MediaTiers.NEARLINE.toString
      }

      "fail with Left if nearline object couldn't be deleted" in {
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
        mockObject.delete throws new IOException("deletion fail")

        val msgObj = getOnlineOutputMessageWithMaybeNearlineId(Some("mxs-22"))

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override def dealWithAttFiles(vault: Vault, nearlineId: String, originalFilePath: String): Future[Either[String, String]] =
            Future(Right(s"The ATT files that were present for ${msgObj.nearlineId} have been deleted"))
        }

        val result =
          Await.result(toTest.deleteMediaFromNearline(mockVault, msgObj.mediaTier, msgObj.originalFilePath, msgObj.nearlineId, msgObj.vidispineItemId), 2.seconds)

        result must beLeft
        result.left.get mustEqual "Failed to remove nearline media oid=mxs-22, path=/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4, reason: deletion fail"
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

      "return original path if putBackBase fails" in {
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

        val mockAssetFolderLookup = mock[AssetFolderLookup]
        mockAssetFolderLookup.putBackBase(any) returns Left("putBackBase-failure")

        val toTest = new NearlineHelper(mockAssetFolderLookup)

        val filePath = "srv/Multimedia2/NextGenDev/Proxies/VX-12074.mp4"
        val result = toTest.putItBack(filePath)

        result mustNotEqual basePath + filePath
        result mustEqual filePath
      }
    }

  "NearlineHelper.findMatchingFilesOnVault" should {
    "should find the file(s) fine if no null sizes and there is at least one match" in {

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
      val entries = Seq(
        ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 30L)), None),
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
        ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 20L)), None),
        ObjectMatrixEntry("08b2307c5c38", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
      )

      val toTest = new NearlineHelper(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
      }

      val result = Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 10L), 2.seconds)

      result.size mustEqual 2
      result.head.oid mustEqual "abd81f4f6c0c"
      result(1).oid mustEqual "08b2307c5c38"
    }

    "return an empty list if no size matches and no null sizes" in {

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
      val entries = Seq(
        ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 30L)), None),
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
        ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 20L)), None),
      )

      val toTest = new NearlineHelper(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
      }

      val result = Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 40L), 2.seconds)

      result.size mustEqual 0
    }

    "return an empty list if no name matches and no null sizes" in {

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
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(Seq())
      }

      val result = Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 40L), 2.seconds)

      result.size mustEqual 0
    }

    "should find the file fine if only some null sizes" in {

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
      val entries = Seq(
        ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 30L)), None),
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
        ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath)), None),
      )

      val toTest = new NearlineHelper(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
      }

      val result = Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 10L), 2.seconds)

      result.size mustEqual 1
      result.head.oid mustEqual "abd81f4f6c0c"
    }

    "should bail out if no matching file, and at least one null size" in {

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
      val entries = Seq(
        ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 30L)), None),
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath).withValue("__mxs__length", 10L)), None),
        ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath)), None),
      )

      val toTest = new NearlineHelper(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
      }

      val result = Try { Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 40L), 2.seconds) }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[BailOutException]
      result.failed.get.getMessage mustEqual "Could not check for matching files of /path/to/some/file.ext because 1 / 3 had no size"
    }

    "should bail out if only null sizes" in {

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
      val entries = Seq(
        ObjectMatrixEntry("556593b10503", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath)), None),
        ObjectMatrixEntry("abd81f4f6c0c", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath)), None),
        ObjectMatrixEntry("b3bcb2fa2146", Some(MxsMetadata.empty.withValue("MXFS_PATH", filePath)), None),
      )

      val toTest = new NearlineHelper(mockAssetFolderLookup) {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(entries)
      }

      val result = Try { Await.result(toTest.findMatchingFilesOnVault(MediaTiers.NEARLINE, vault, filePath, 10L), 2.seconds) }

      println(s"<<<< $result")
      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Could not check for matching files of /path/to/some/file.ext because 3 / 3 had no size"
    }

  }

}
