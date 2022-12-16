import akka.actor.ActorSystem
import akka.stream.Materializer

import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, RMQDestination, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.{OnlineOutputMessage, VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.models.online_archive.ArchivedRecord
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import messages.MediaRemovedMessage
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try

class OnlineArchiveMessageProcessorSpec extends Specification with Mockito {
  implicit val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

  private def makeArchiveRecord(id: Int, originalFilePath: String, uploadedFilePath: String, vsItemIdMaybe: Some[String]) =
    ArchivedRecord(Some(id), "abcdefg", archiveHunterIDValidated = false, originalFilePath, 100, "testbucket", uploadedFilePath, None, vsItemIdMaybe, None, None, None, None, None, None, UUID.randomUUID().toString)


  "OnlineArchiveMessageProcessor.findPendingDeletionRecord" should {
    "find first if present" in {
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]


      val archivedRecord = makeArchiveRecord(1234, "original/file/path/some.file", "uploaded/path/some.file", Some("VX-1"))

      val rec1 = PendingDeletionRecord(None, "original/file/path/some.file", Some("nearline-test-id-to-delete"), Some("VX-1"), MediaTiers.NEARLINE, 1)
      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(any, any) returns Future(None)
      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier("original/file/path/some.file", MediaTiers.NEARLINE) returns Future(Some(rec1))

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.findPendingDeletionRecord(archivedRecord), 2.seconds)

      result.size mustEqual 1
      result.get.vidispineItemId must beSome("VX-1")
    }

    "find second if present" in {
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val archivedRecord = makeArchiveRecord(1235, "some/other/original/file/path/some.file", "uploaded/path/some.file", Some("VX-3"))

      val rec = PendingDeletionRecord(None, "uploaded/path/some.file", Some("nearline-test-id-to-delete"), Some("VX-3"), MediaTiers.NEARLINE, 1)

      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier(any, any) returns Future(None)
      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier("uploaded/path/some.file", MediaTiers.NEARLINE) returns Future(Some(rec))

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.findPendingDeletionRecord(archivedRecord), 2.seconds)

      result.size mustEqual 1
      result.get.vidispineItemId must beSome("VX-3")
    }

    "find first if both present" in {
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]

      val vault = mock[Vault]
      vault.getId returns "mockVault"

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val archivedRecord = makeArchiveRecord(1234, "original/file/path/some.file", "uploaded/path/some.file", Some("VX-5"))

      val rec = PendingDeletionRecord(None, "original/file/path/some.file", Some("nearline-test-id-to-delete"), Some("VX-5"), MediaTiers.NEARLINE, 1)
      val rec2 = PendingDeletionRecord(None, "uploaded/path/some.file", Some("nearline-test-id-to-delete"), Some("VX-6"), MediaTiers.NEARLINE, 1)


      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier("original/file/path/some.file", MediaTiers.NEARLINE) returns Future(Some(rec))
      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier("uploaded/path/some.file", MediaTiers.NEARLINE) returns Future(Some(rec2))

      val result = Await.result(toTest.findPendingDeletionRecord(archivedRecord), 2.seconds)

      result.size mustEqual 1
      result.get.vidispineItemId must beSome("VX-5")
    }

    "find none if neither present" in {
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      implicit val mockPendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
      implicit val mockChecksumChecker = mock[ChecksumChecker]
      implicit val mockOnlineHelper = mock[OnlineHelper]
      implicit val mockNearlineHelper = mock[NearlineHelper]
      implicit val mockPendingDeletionHelper = mock[PendingDeletionHelper]
      val mockAssetFolderLookup = mock[AssetFolderLookup]


      val archivedRecord = makeArchiveRecord(1234, "original/file/path/some.file", "uploaded/path/some.file", Some("VX-7"))

      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier("original/file/path/some.file", MediaTiers.NEARLINE) returns Future(None)
      mockPendingDeletionRecordDAO.findBySourceFilenameAndMediaTier("uploaded/path/some.file", MediaTiers.NEARLINE) returns Future(None)

      val toTest = new OnlineArchiveMessageProcessor(mockAssetFolderLookup)

      val result = Await.result(toTest.findPendingDeletionRecord(archivedRecord), 2.seconds)

      result.size mustEqual 0
      result must beNone
    }
  }
}




