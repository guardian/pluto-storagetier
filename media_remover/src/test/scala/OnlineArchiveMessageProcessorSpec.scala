import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl

import scala.concurrent.ExecutionContext.Implicits.global
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.SilentDropMessage
import com.gu.multimedia.storagetier.messages.VidispineMediaIngested
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecordDAO
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.plutocore.AssetFolderLookup
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.Vault
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.mockito.ArgumentMatchersSugar.eqTo
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try

//noinspection ScalaDeprecation
class OnlineArchiveMessageProcessorSpec extends Specification with Mockito {
  implicit val mxsConfig: MatrixStoreConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

  private def makeArchiveRecord(originalFilePath: String, uploadedFilePath: String, vsItemIdMaybe: Option[String]): ArchivedRecord =
    ArchivedRecord(
      id = Some(1234),
      archiveHunterID = "abcdefg",
      archiveHunterIDValidated = false,
      originalFilePath = originalFilePath,
      originalFileSize = 100,
      uploadedBucket = "test-bucket",
      uploadedPath = uploadedFilePath,
      uploadedVersion = None,
      vidispineItemId = vsItemIdMaybe,
      vidispineVersionId = None,
      proxyBucket = None,
      proxyPath = None,
      proxyVersion = None,
      metadataXML = None,
      metadataVersion = None,
      correlationId = UUID.randomUUID().toString)


  "OnlineArchiveMessageProcessor.handleDeepArchiveCompleteForOnline" should {
    "send Left if no vsItemId in archived record" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val archivedRec = makeArchiveRecord("original/file/path/some.file", "uploaded/path/some.file", None)
      val pendingRec = PendingDeletionRecord(None, "original/file/path/some.file", None, Some("VX-1"), MediaTiers.ONLINE, 1)

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(any) returns Future(None)
      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE("original/file/path/some.file") returns Future(Some(pendingRec))

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.handleDeepArchiveCompleteForOnline(archivedRec), 2.seconds)

      result must beLeft("Although deep archive is complete for ONLINE item with original/file/path/some.file, the archive record is missing the vsItemId which we need to do the deletion")
    }

    "silentDrop if no pending deletion found for online item" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val archivedRec = makeArchiveRecord("original/file/path/some.file", "uploaded/path/some.file", Some("VX-1"))

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(any) returns Future(None)

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Try { Await.result(toTest.handleDeepArchiveCompleteForOnline(archivedRec), 2.seconds) }

      result must beFailedTry(SilentDropMessage(Some("ignoring archive confirmation, no pending deletion for this ONLINE item with original/file/path/some.file")))
    }

    "send Left if we couldn't connect to S3" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val archivedRec = makeArchiveRecord("original/file/path/some.file", "uploaded/path/some.file", Some("VX-1"))
      val pendingRec = PendingDeletionRecord(None, "original/file/path/some.file", None, Some("VX-1"), MediaTiers.ONLINE, 1)

      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE(any) returns Future(None)
      mockPendingDeletionRecordDAO.findByOnlineIdForONLINE("VX-1") returns Future(Some(pendingRec))

      mockOnlineHelper.getOnlineSize("VX-1") returns Future(Some(1024L))
      mockOnlineHelper.getMd5ChecksumForOnline(any) returns Future(Some("fake-MD5"))

      mockS3ObjectChecker.onlineMediaExistsInDeepArchive(any, any, any, any) throws new RuntimeException("Can't connect to S3!")

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.handleDeepArchiveCompleteForOnline(archivedRec), 2.seconds)

      result must beLeft("Could not connect to deep archive to check if copy of ONLINE media exists, do not delete. Reason: Can't connect to S3!")
    }
  }


  "OnlineArchiveMessageProcessor.handleDeepArchiveCompleteForNearline" should {
    "send DLQ if no itemId" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val archivedRec = makeArchiveRecord("original/file/path/some.file", "uploaded/path/some.file", Some("VX-1"))
      val pendingRec = PendingDeletionRecord(None, "original/file/path/some.file", None, Some("VX-1"), MediaTiers.NEARLINE, 1)

      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE(any) returns Future(None)
      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE("original/file/path/some.file") returns Future(Some(pendingRec))
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(any) returns Future(Seq())
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE("original/file/path/some.file") returns Future(Seq(pendingRec))

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Try {Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)}

      result must beFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "No nearline ID in pending deletion rec for media original/file/path/some.file"
    }

    "send Left if one pending deletion but cannot connect to S3" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val anOriginalFilePath = "original/file/path/some.file"
      val anUploadedPath = "uploaded/path/some.file"
      val archivedRec = makeArchiveRecord(anOriginalFilePath, anUploadedPath, Some("VX-1"))
      val pendingRec = PendingDeletionRecord(None, anOriginalFilePath, Some("mxs-1"), Some("VX-1"), MediaTiers.NEARLINE, 1)
      val aChecksum = "07ce8816e0217b02568ef03612fc6207"

      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE(any) returns Future(None)
      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Some(pendingRec))
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(any) returns Future(Seq())
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Seq(pendingRec))

      mockNearlineHelper.getNearlineFileSize(any, any) returns Future(50L)
      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some(aChecksum))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns
        Future(Right(MediaRemovedMessage("NEARLINE", anOriginalFilePath, Some("VX-1"), Some("mxs-1")).asJson))

      mockPendingDeletionRecordDAO.deleteRecord(any) returns Future(1)

      // Poor man's validation - tests throws an error if this mock isn't matched
      mockS3ObjectChecker.nearlineMediaExistsInDeepArchive(Some(aChecksum), 50L, anOriginalFilePath, anUploadedPath) returns Future(true)

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup) {
        override def getNearlineDataAndCheckExistsInDeepArchive(vault: Vault, archivedRec: ArchivedRecord, pendingRec: PendingDeletionRecord, nearlineId: String): Future[Boolean] = Future.failed(new RuntimeException("S3-fail"))
      }

      val result = Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)

      result must beLeft("Could not connect to deep archive to check if copy of NEARLINE media exists, do not delete. Reason: S3-fail")
    }

    "delete item if one pending deletion and exists" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val anOriginalFilePath = "original/file/path/some.file"
      val anUploadedPath = "uploaded/path/some.file"
      val archivedRec = makeArchiveRecord(anOriginalFilePath, anUploadedPath, Some("VX-1"))
      val pendingRec = PendingDeletionRecord(None, anOriginalFilePath, Some("mxs-1"), Some("VX-1"), MediaTiers.NEARLINE, 1)
      val aChecksum = "07ce8816e0217b02568ef03612fc6207"

      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE(any) returns Future(None)
      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Some(pendingRec))
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(any) returns Future(Seq())
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Seq(pendingRec))

      mockNearlineHelper.getNearlineFileSize(any, any) returns Future(50L)
      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some(aChecksum))
      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns
        Future(Right(MediaRemovedMessage("NEARLINE", anOriginalFilePath, Some("VX-1"), Some("mxs-1")).asJson))

      mockPendingDeletionRecordDAO.deleteRecord(any) returns Future(1)

      // Poor man's validation - tests throws an error if this mock isn't matched
      mockS3ObjectChecker.nearlineMediaExistsInDeepArchive(Some(aChecksum), 50L, anOriginalFilePath, anUploadedPath) returns Future(true)

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)

      result must beRight
      val mediaRemovedMessage = result.right.get.content.as[MediaRemovedMessage].right.get
      mediaRemovedMessage.vidispineItemId must beSome("VX-1")
      mediaRemovedMessage.nearlineId must beSome("mxs-1")
      mediaRemovedMessage.originalFilePath mustEqual anOriginalFilePath
      mediaRemovedMessage.mediaTier mustEqual MediaTiers.NEARLINE.toString


    }

    "delete one item if two pending deletions and one of their items exist" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val anOriginalFilePath = "original/file/path/some.file"
      val anUploadedPath = "uploaded/path/some.file"
      val aChecksum = "07ce8816e0217b02568ef03612fc6207"
      val archivedRec = makeArchiveRecord(anOriginalFilePath, anUploadedPath, Some("VX-3"))
      val pendingRec = PendingDeletionRecord(None, anOriginalFilePath, Some("mxs-5"), Some("VX-3"), MediaTiers.NEARLINE, 1)
      val pendingRec2 = PendingDeletionRecord(None, anOriginalFilePath, Some("mxs-4"), Some("VX-3"), MediaTiers.NEARLINE, 1)

      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Seq(pendingRec, pendingRec2))

      mockNearlineHelper.getNearlineFileSize(any, any) returns Future(50L)
      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some(aChecksum))

      // Poor man's validation - test throws an error if this mock isn't matched
      mockS3ObjectChecker.nearlineMediaExistsInDeepArchive(Some(aChecksum), 50L, anOriginalFilePath, anUploadedPath) returns Future(true)

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mock[Json]))
      mockPendingDeletionRecordDAO.deleteRecord(any) returns Future(1)

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)

      there was atLeast(1)(mockS3ObjectChecker).nearlineMediaExistsInDeepArchive(any, any, any, any)

      there was one(mockNearlineHelper).deleteMediaFromNearline(
        vault = eqTo(mockVault),
        mediaTier = eqTo(MediaTiers.NEARLINE.toString),
        filePathMaybe = eqTo(Some(anOriginalFilePath)),
        nearlineIdMaybe = any,
        vidispineItemIdMaybe = eqTo(Some("VX-3")))

      result must beRight
    }

    "return Left if two pending deletions and at least one of the S3 checks throws an exception" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val anOriginalFilePath = "original/file/path/some.file"
      val anUploadedPath = "uploaded/path/some.file"
      val aChecksum = "07ce8816e0217b02568ef03612fc6207"
      val archivedRec = makeArchiveRecord(anOriginalFilePath, anUploadedPath, Some("VX-3"))
      val pendingRec = PendingDeletionRecord(Some(1), anOriginalFilePath, Some("mxs-5"), Some("VX-3"), MediaTiers.NEARLINE, 1)
      val pendingRec2 = PendingDeletionRecord(Some(2), anOriginalFilePath, Some("mxs-4"), Some("VX-3"), MediaTiers.NEARLINE, 1)

      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Seq(pendingRec, pendingRec2))

      mockNearlineHelper.getNearlineFileSize(any, any) returns Future(50L)
      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some(aChecksum))

      // Poor man's validation - test throws an error if this mock isn't matched
      mockS3ObjectChecker.nearlineMediaExistsInDeepArchive(Some(aChecksum), 50L, anOriginalFilePath, anUploadedPath) returns Future(true)

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mock[Json]))
      mockPendingDeletionRecordDAO.deleteRecord(any) returns Future(1)

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup) {
        override def getNearlineDataAndCheckExistsInDeepArchive(vault: Vault, archivedRec: ArchivedRecord, pendingRec: PendingDeletionRecord, nearlineId: String): Future[Boolean] =
          Future.failed(new RuntimeException("S3-fail multi"))
      }

      val result = Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)

      result must beLeft("Could not connect to deep archive to check if copy of NEARLINE media exists, do not delete. Reason: S3-fail multi")
    }

    "update one pdr and send one copy request if two pending deletions and none of their items exist" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val anOriginalFilePath = "original/file/path/some.file"
      val anUploadedPath = "uploaded/path/some.file"
      val aChecksum = "07ce8816e0217b02568ef03612fc6207"

      val archivedRec = makeArchiveRecord(anOriginalFilePath, anUploadedPath, Some("VX-3"))
      val pendingRec = PendingDeletionRecord(Some(1), anOriginalFilePath, Some("mxs-5"), Some("VX-3"), MediaTiers.NEARLINE, 1)
      val pendingRec2 = PendingDeletionRecord(Some(2), anOriginalFilePath, Some("mxs-4"), Some("VX-3"), MediaTiers.NEARLINE, 1)

      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Seq(pendingRec, pendingRec2))

      mockNearlineHelper.getNearlineFileSize(any, any) returns Future(50L)
      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some(aChecksum))

      // Poor man's validation - test throws an error if this mock isn't matched
      mockS3ObjectChecker.nearlineMediaExistsInDeepArchive(Some(aChecksum), 50L, anOriginalFilePath, anUploadedPath) returns Future(false)

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mock[Json]))
      mockPendingDeletionRecordDAO.deleteRecord(any) returns Future(1)


      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)


      val result = Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)

      there was atLeast(1)(mockS3ObjectChecker).nearlineMediaExistsInDeepArchive(any, any, any, any)

      there was no(mockNearlineHelper).deleteMediaFromNearline(any, any, any, any, any)

      there was one(mockPendingDeletionRecordDAO).updateAttemptCount(any, any)
      there was one(mockPendingDeletionRecordDAO).updateAttemptCount(any, eqTo(2))

      result must beRight
      val mprv = result.getOrElse(null)
      mprv.content.as[VidispineMediaIngested].right.get.itemId must beSome("VX-3")
      mprv.additionalDestinations.length mustEqual 1
      mprv.additionalDestinations.head.outputExchange mustEqual "vidispine-events"
      mprv.additionalDestinations.head.routingKey mustEqual "vidispine.itemneedsarchive.nearline"
    }

    "fail if more than one pending deletion, no match is found, and none of the records have a vidispine ID, meaning we can't request a deep archive copy" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val anOriginalFilePath = "original/file/path/some.file"
      val anUploadedPath = "uploaded/path/some.file"
      val aChecksum = "07ce8816e0217b02568ef03612fc6207"

      val archivedRec = makeArchiveRecord(anOriginalFilePath, anUploadedPath, Some("VX-3"))
      val pendingRec = PendingDeletionRecord(Some(1), anOriginalFilePath, Some("mxs-5"), None, MediaTiers.NEARLINE, 1)
      val pendingRec2 = PendingDeletionRecord(Some(2), anOriginalFilePath, Some("mxs-4"), None, MediaTiers.NEARLINE, 1)

      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE(anOriginalFilePath) returns Future(Seq(pendingRec, pendingRec2))

      mockNearlineHelper.getNearlineFileSize(any, any) returns Future(50L)
      mockNearlineHelper.getChecksumForNearline(any, any) returns Future(Some(aChecksum))

      // Poor man's validation - test throws an error if this mock isn't matched
      mockS3ObjectChecker.nearlineMediaExistsInDeepArchive(Some(aChecksum), 50L, anOriginalFilePath, anUploadedPath) returns Future(false)

      mockNearlineHelper.deleteMediaFromNearline(any, any, any, any, any) returns Future(Right(mock[Json]))
      mockPendingDeletionRecordDAO.deleteRecord(any) returns Future(1)


      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)


      val result = Try { Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds) }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "Cannot request deep archive copy for NEARLINE, original/file/path/some.file, none of the matching pending deletion records has a vidispine ID!"
    }

    "silentDrop if no pending deletion found" in {
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val mockNearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val mockVidispineCommunicator: VidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockBuilder: MXSConnectionBuilderImpl = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker: S3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockOnlineHelper: OnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper: NearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper: PendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]
      val mockVault = mock[Vault]

      val archivedRec = makeArchiveRecord("original/file/path/some.file", "uploaded/path/some.file", Some("VX-1"))

      mockPendingDeletionRecordDAO.findByOriginalFilePathForNEARLINE(any) returns Future(None)
      mockPendingDeletionRecordDAO.findAllByOriginalFilePathForNEARLINE("original/file/path/some.file") returns Future(Seq())

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Try {Await.result(toTest.handleDeepArchiveCompleteForNearline(mockVault, archivedRec), 2.seconds)}

      result must beFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]
      result.failed.get.getMessage mustEqual "ignoring archive confirmation, no pending deletion for this NEARLINE item with original/file/path/some.file"
    }
  }
}




