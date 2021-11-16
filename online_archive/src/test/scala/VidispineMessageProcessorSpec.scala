import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, HttpEntity}
import akka.stream.Materializer
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.scaladsl.Source
import akka.util.ByteString
import io.circe.syntax.EncoderOps
import archivehunter.ArchiveHunterCommunicator
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecord, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{FileDocument, ShapeDocument, SimplifiedComponent, VSShapeFile, VidispineCommunicator}
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, PlutoCoreConfig, ProductionOffice, ProjectRecord}
import plutodeliverables.PlutoDeliverablesConfig
import utils.ArchiveHunter
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.{VidispineField, VidispineMediaIngested}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import java.nio.file.{Path, Paths}
import scala.concurrent.duration.DurationInt
import scala.util.{Success, Try}
import java.io.InputStream
import java.time.ZonedDateTime

class VidispineMessageProcessorSpec extends Specification with Mockito {
  val fakeDeliverablesConfig = PlutoDeliverablesConfig("Deliverables/",1)
  "VidispineMessageProcessor.uploadIfRequiredAndNotExists" should {
    "return a success message without an upload attempt but setting the vidispine item id" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val mockMediaUploader = mock[FileUploader]
      mockMediaUploader.bucketName returns "bucket"
      mockMediaUploader.objectExists(any) returns Success(true)

      implicit val mockProxyUploader = mock[FileUploader]

      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      ignoredRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      val basePath = Paths.get("/media/assets")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath), fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)
      val fields: List[VidispineField] = List(VidispineField("originalPath", "original/path"), VidispineField("itemId",
        "VX-123"), VidispineField("bytesWritten", "12345"), VidispineField("status", "FINISHED"))
      val ingested = VidispineMediaIngested(fields)

      val record = ArchivedRecord(
        archiveHunterID="archiveId",
        originalFilePath="/media/file.mp4",
        originalFileSize=12345,
        uploadedBucket="bucket",
        uploadedPath="uploaded/path",
        uploadedVersion=Some(4)
      ).copy(archiveHunterIDValidated = true)
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(record))

      val result = Await.result(toTest.uploadIfRequiredAndNotExists("/media/file.mp4", "file.mp4", ingested), 2.seconds)
      there was one(mockMediaUploader).objectExists("uploaded/path")
      there was no(mockMediaUploader).copyFileToS3(any,any)
      there was no(mockMediaUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was no(mockProxyUploader).objectExists(any)
      there was no(mockProxyUploader).copyFileToS3(any,any)
      there was no(mockProxyUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was one(archivedRecordDAO).writeRecord(argThat((rec:ArchivedRecord)=>rec.vidispineItemId.contains("VX-123")))
      there was no(ignoredRecordDAO).writeRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
      result must beRight
    }

    "use the full path for upload if it can't relativize" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val mockUploader = mock[FileUploader]

      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]

      val basePath = Paths.get("/dummy/base/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath), fakeDeliverablesConfig, mockUploader, mockUploader)

      toTest.getRelativePath("/some/totally/other/path", None) must beLeft()
    }

    "VidispineMessageProcessor.handleIngestedMedia" should {
      "fail request when job status includes FAIL" in {
        implicit val mockVSCommunicator = mock[VidispineCommunicator]
        implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
        implicit val mockUploader = mock[FileUploader]
        implicit val archivedRecordDAO: ArchivedRecordDAO = mock[ArchivedRecordDAO]
        archivedRecordDAO.writeRecord(any) returns Future(123)
        implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
        failureRecordDAO.writeRecord(any) returns Future(234)
        implicit val ignoredRecordDAO: IgnoredRecordDAO = mock[IgnoredRecordDAO]
        ignoredRecordDAO.writeRecord(any) returns Future(345)
        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        val mediaIngested = VidispineMediaIngested(List(
          VidispineField("itemId", "VX-123"),
          VidispineField("bytesWritten", "100"),
          VidispineField("status", "FAILED"),
          VidispineField("sourceFileId", "VX-456"),
          VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
        ))

        val basePath = Paths.get("/dummy/base/path")
        val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server", "notsecret", basePath), fakeDeliverablesConfig, mockUploader, mockUploader)

        val result = Try {
          Await.result(toTest.handleIngestedMedia(mediaIngested), 3.seconds)
        }

        result must beFailedTry
      }

      "call out to uploadIfRequiredAndNotExists" in {
        val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2", None)
        implicit val mockVSCommunicator = mock[VidispineCommunicator]
        mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))
        implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
        implicit val mockUploader = mock[FileUploader]
        implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
        archivedRecordDAO.writeRecord(any) returns Future(123)
        implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
        failureRecordDAO.writeRecord(any) returns Future(234)
        implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
        ignoredRecordDAO.writeRecord(any) returns Future(345)
        implicit val mat:Materializer = mock[Materializer]
        implicit val sys:ActorSystem = mock[ActorSystem]
        val mediaIngested = VidispineMediaIngested(List(
          VidispineField("itemId", "VX-123"),
          VidispineField("bytesWritten", "12345"),
          VidispineField("status", "FINISHED"),
          VidispineField("sourceFileId", "VX-456"),
          VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
        ))

        val mockUploadIfReqd = mock[(String, String, VidispineMediaIngested)=>Future[Either[String,MessageProcessorReturnValue]]]
        val fakeResult = mock[Json]
        mockUploadIfReqd.apply(any,any,any) returns Future(Right(fakeResult))

        val mockedProject = ProjectRecord(
          Some(1234), 2, "Test project", ZonedDateTime.now(), ZonedDateTime.now(), "someuser", None,None,Some(false),None, Some(false), EntryStatus.New, ProductionOffice.UK
        )

        val basePath = Paths.get("/absolute/path")
        val fakePlutoConfig = PlutoCoreConfig("https://fake-server","notsecret",basePath)
        val realASlookup = new AssetFolderLookup(fakePlutoConfig)
        val mockAsLookup = mock[AssetFolderLookup]
        mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(mockedProject))
        mockAsLookup.relativizeFilePath(any) answers ((arg:Any)=>realASlookup.relativizeFilePath(arg.asInstanceOf[Path]))

        val toTest = new VidispineMessageProcessor(fakePlutoConfig, fakeDeliverablesConfig, mockUploader, mockUploader) {
          override protected lazy val asLookup: AssetFolderLookup = mockAsLookup
          override def uploadIfRequiredAndNotExists(filePath: String, relativePath: String, mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfReqd(filePath, relativePath, mediaIngested)
        }

        val result = Await.result(toTest.handleIngestedMedia(mediaIngested), 2.seconds)
        result must beRight
        result.right.get.content mustEqual fakeResult
        there was one(mockUploadIfReqd).apply("/absolute/path/relative/path.mp4","relative/path.mp4",mediaIngested)
      }

      "fall back to fileId if sourceFileId not set" in {
        val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2", None)
        implicit val mockVSCommunicator = mock[VidispineCommunicator]
        mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))
        implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
        implicit val mockUploader = mock[FileUploader]
        implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
        archivedRecordDAO.writeRecord(any) returns Future(123)
        implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
        failureRecordDAO.writeRecord(any) returns Future(234)
        implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
        ignoredRecordDAO.writeRecord(any) returns Future(345)
        implicit val mat:Materializer = mock[Materializer]
        implicit val sys:ActorSystem = mock[ActorSystem]
        val mediaIngested = VidispineMediaIngested(List(
          VidispineField("itemId", "VX-123"),
          VidispineField("bytesWritten", "12345"),
          VidispineField("status", "FINISHED"),
          VidispineField("fileId", "VX-456"),
          VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
        ))

        val mockUploadIfReqd = mock[(String, String, VidispineMediaIngested)=>Future[Either[String,Json]]]
        val fakeResult = mock[Json]
        mockUploadIfReqd.apply(any,any,any) returns Future(Right(fakeResult))

        val mockedProject = ProjectRecord(
          Some(1234), 2, "Test project", ZonedDateTime.now(), ZonedDateTime.now(), "someuser", None,None,Some(false),Some(true), Some(false), EntryStatus.New, ProductionOffice.UK
        )

        val basePath = Paths.get("/absolute/path")
        val fakePlutoConfig = PlutoCoreConfig("https://fake-server","notsecret",basePath)
        val realASlookup = new AssetFolderLookup(fakePlutoConfig)
        val mockAsLookup = mock[AssetFolderLookup]
        mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(mockedProject))
        mockAsLookup.relativizeFilePath(any) answers ((arg:Any)=>realASlookup.relativizeFilePath(arg.asInstanceOf[Path]))

        val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath), fakeDeliverablesConfig, mockUploader, mockUploader) {
          override protected lazy val asLookup = mockAsLookup
          override def uploadIfRequiredAndNotExists(filePath: String, relativePath: String, mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfReqd(filePath, relativePath, mediaIngested)
        }

        val result = Await.result(toTest.handleIngestedMedia(mediaIngested), 2.seconds)
        there was one(mockUploadIfReqd).apply("/absolute/path/relative/path.mp4","relative/path.mp4",mediaIngested)
        result must beRight
        result.right.get.content mustEqual(fakeResult)
      }
    }

    "SilentDrop if the project is deletable" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2", None)
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val mockUploader = mock[FileUploader]
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "12345"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val mockUploadIfReqd = mock[(String, String, VidispineMediaIngested)=>Future[Either[String,MessageProcessorReturnValue]]]
      val fakeResult = mock[Json]
      mockUploadIfReqd.apply(any,any,any) returns Future(Right(fakeResult))

      val mockedProject = ProjectRecord(
        Some(1234), 2, "Test project", ZonedDateTime.now(), ZonedDateTime.now(), "someuser", None,None,Some(true),None, Some(false), EntryStatus.New, ProductionOffice.UK
      )

      val mockAsLookup = mock[AssetFolderLookup]
      mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(mockedProject))
      val basePath = Paths.get("/absolute/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath), fakeDeliverablesConfig, mockUploader, mockUploader) {
        override protected lazy val asLookup: AssetFolderLookup = mockAsLookup
        override def uploadIfRequiredAndNotExists(filePath: String, relativePath: String, mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfReqd(filePath, relativePath, mediaIngested)
      }

      val result = Try { Await.result(toTest.handleIngestedMedia(mediaIngested), 2.seconds) }
      result must beAFailedTry(SilentDropMessage(Some("/absolute/path/relative/path.mp4 is from project Some(1234) which is either deletable or sensitive")))
    }

    "SilentDrop if the project is sensitive" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2", None)
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val mockUploader = mock[FileUploader]
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "12345"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val mockUploadIfReqd = mock[(String, String, VidispineMediaIngested)=>Future[Either[String,MessageProcessorReturnValue]]]
      val fakeResult = mock[Json]
      mockUploadIfReqd.apply(any,any,any) returns Future(Right(fakeResult))

      val mockedProject = ProjectRecord(
        Some(1234), 2, "Test project", ZonedDateTime.now(), ZonedDateTime.now(), "someuser", None,None,Some(false),None, Some(true), EntryStatus.New, ProductionOffice.UK
      )

      val mockAsLookup = mock[AssetFolderLookup]
      mockAsLookup.assetFolderProjectLookup(any) returns Future(Some(mockedProject))
      val basePath = Paths.get("/absolute/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath), fakeDeliverablesConfig, mockUploader, mockUploader) {
        override protected lazy val asLookup: AssetFolderLookup = mockAsLookup
        override def uploadIfRequiredAndNotExists(filePath: String, relativePath: String, mediaIngested: VidispineMediaIngested): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfReqd(filePath, relativePath, mediaIngested)
      }

      val result = Try { Await.result(toTest.handleIngestedMedia(mediaIngested), 2.seconds) }
      result must beAFailedTry(SilentDropMessage(Some("/absolute/path/relative/path.mp4 is from project Some(1234) which is either deletable or sensitive")))
    }
  }

  "VidispineMessageProcessor.uploadKeyForProxy" should {
    "generate a '_prox' filename based on the original media path with the proxy extension" in {
      val testArchivedRecord = ArchivedRecord(
        "some-archivehunter-id",
        "/some/path/to/original/file",
        123456L,
        "some-bucket",
        "path/to/uploaded/file.mxf",
        None
      )
      val testProxy = VSShapeFile(
        "VX-1234",
        "another/location/for/proxies/VX-1234.mp4",
        Seq("file:///srv/proxies/another/location/for/proxies/VX-1234.mp4"),
        "CLOSED",
        1234L,
        Some("deadbeef"),
        "2021-01-02T03:04:05.678Z",
        1,
        "VX-2"
      )
      VidispineMessageProcessor.uploadKeyForProxy(testArchivedRecord, testProxy) mustEqual "path/to/uploaded/file_prox.mp4"
    }
    "not mess up with silly data" in {
      val testArchivedRecord = ArchivedRecord(
        "some-archivehunter-id",
        "/some/path/to/original/file",
        123456L,
        "some-bucket",
        "path/to/uploaded/file.mxf",
        None
      )
      val testProxy = VSShapeFile(
        "VX-1234",
        "another/location/for/proxies/VX-1234.mp4",
        Seq(),
        "CLOSED",
        1234L,
        Some("deadbeef"),
        "2021-01-02T03:04:05.678Z",
        1,
        "VX-2"
      )
      VidispineMessageProcessor.uploadKeyForProxy(testArchivedRecord, testProxy) mustEqual "path/to/uploaded/file_prox"
    }
  }

  "VidispineMessageProcessor.handleShapeUpdate" should {
    "call out to uploadShapeIfRequired provided that the shape should be pushed" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)
      val mockUploadShapeIfRequired = mock[(String, String, String, ArchivedRecord)=>Future[Either[String, Json]]]
      mockUploadShapeIfRequired.apply(any,any,any,any) returns Future(Right(mockArchivedRecord.asJson))

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.akkaStreamFirstThumbnail(any,any) returns Future(None)

      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockFileUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockFileUploader, mockFileUploader) {
        override def uploadShapeIfRequired(itemId: String, shapeId: String, shapeTag: String, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
          mockUploadShapeIfRequired(itemId, shapeId, shapeTag, archivedRecord)
      }

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beRight
      result.right.get.content mustEqual (mockArchivedRecord.asJson)
      there was one(mockUploadShapeIfRequired).apply("VX-123","VX-456", "lowres", mockArchivedRecord)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }

    "not call out to uploadShapeIfRequired and return Left if the ArchiveHunter ID is not validated yes" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = false)
      val mockUploadShapeIfRequired = mock[(String, String, String, ArchivedRecord)=>Future[Either[String, Json]]]
      mockUploadShapeIfRequired.apply(any,any,any,any) returns Future(Right(mockArchivedRecord.asJson))

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockFileUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockFileUploader, mockFileUploader) {
        override def uploadShapeIfRequired(itemId: String, shapeId: String, shapeTag: String, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
          mockUploadShapeIfRequired(itemId, shapeId, shapeTag, archivedRecord)
      }

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beLeft("ArchiveHunter ID for /path/to/original/file has not been validated yet")
      there was no(mockUploadShapeIfRequired).apply("VX-123","VX-456", "lowres", mockArchivedRecord)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }

    "not call out to uploadShapeIfRequired and return Right if the file should be ignored" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)
      val mockIgnoredRecord = IgnoredRecord(None,"/path/to/original/file", "test", Some("VX-123"), None)

      val mockUploadShapeIfRequired = mock[(String, String, String, ArchivedRecord)=>Future[Either[String, Json]]]
      mockUploadShapeIfRequired.apply(any,any,any,any) returns Future(Right(mockArchivedRecord.asJson))

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(Some(mockIgnoredRecord))
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockFileUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockFileUploader, mockFileUploader) {
        override def uploadShapeIfRequired(itemId: String, shapeId: String, shapeTag: String, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
          mockUploadShapeIfRequired(itemId, shapeId, shapeTag, archivedRecord)
      }

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beRight
      result.right.get.content mustEqual (mockIgnoredRecord.asJson)
      there was no(mockUploadShapeIfRequired).apply("VX-123","VX-456", "lowres", mockArchivedRecord)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }

    "not call out to uploadShapeIfRequired and return Left if the item in question is neither archived nor ignored" in {
      val mockUploadShapeIfRequired = mock[(String, String, String, ArchivedRecord)=>Future[Either[String, Json]]]

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockFileUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockFileUploader, mockFileUploader) {
        override def uploadShapeIfRequired(itemId: String, shapeId: String, shapeTag: String, archivedRecord: ArchivedRecord): Future[Either[String, MessageProcessorReturnValue]] =
          mockUploadShapeIfRequired(itemId, shapeId, shapeTag, archivedRecord)
      }

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beLeft("No record of vidispine item VX-123")
      there was no(mockUploadShapeIfRequired).apply(any,any,any,any)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }
  }

  "VidispineMessageProcessor.uploadShapeIfRequired" should {
    "stream the file content, tell ArchiveHunter to update its proxy and then update the datastore record" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      val mockedInputStream = mock[InputStream]
      val sampleFile = VSShapeFile("VX-789",
        "VX-789.mp4",
        Seq("file:///path/to/Vidispine/Proxies/VX-789.mp4"),
        "CLOSED",
        1234L,
        Some("deadbeef"),
        "",
        1,
        "VX-3"
      )
      val shapeDoc = ShapeDocument(
        "VX-456",
        "",
        Some(1),
        Seq("lowres"),
        Seq("video/mp4"),
        Some(SimplifiedComponent("VX-111",Seq(sampleFile))),
        Some(Seq(SimplifiedComponent("VX-112",Seq(sampleFile)))),
        Some(Seq(SimplifiedComponent("VX-113",Seq(sampleFile))))
      )

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.streamFileContent(any,any) returns Future(mockedInputStream)
      vidispineCommunicator.findItemShape(any,any) returns Future(Some(shapeDoc))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      mockProxyUploader.bucketName returns "proxy-bucket"
      mockProxyUploader.uploadStreamNoChecks(any,any,any,any,any) returns Success("path/to/uploaded/file_prox.mp4",1234L)
      mockProxyUploader.copyFileToS3(any,any) returns Success("path/to/uploaded/file_prox.mp4", 1234L)
      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader) {
        override protected def internalCheckFile(filePath: Path): Boolean = true
      }

      val result = Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      )

      val outputRecord = mockArchivedRecord.copy(proxyPath = Some("path/to/uploaded/file_prox.mp4"), proxyBucket = Some("proxy-bucket"))
      result must beRight
      result.right.get.content mustEqual (outputRecord.asJson)
      there was no(vidispineCommunicator).streamFileContent(any,any)
      there was one(mockProxyUploader).copyFileToS3(any, org.mockito.ArgumentMatchers.eq(Some("path/to/uploaded/file_prox.mp4")))
      there was no(mockProxyUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was no(mockMediaUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was one(archiveHunterCommunicator).importProxy("abcdefg", "path/to/uploaded/file_prox.mp4", "proxy-bucket", ArchiveHunter.ProxyType.VIDEO)
      there was one(mockArchivedRecordDAO).writeRecord(outputRecord)
    }

    "return Left if there is no file data available in Vidispine" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      val mockedInputStream = mock[InputStream]
      val shapeDoc = ShapeDocument(
        "VX-456",
        "",
        Some(1),
        Seq("lowres"),
        Seq("video/mp4"),
        Some(SimplifiedComponent("VX-111",Seq())),
        Some(Seq(SimplifiedComponent("VX-112",Seq()))),
        Some(Seq(SimplifiedComponent("VX-113",Seq())))
      )

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.streamFileContent(any,any) returns Future(mockedInputStream)
      vidispineCommunicator.findItemShape(any,any) returns Future(Some(shapeDoc))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      mockProxyUploader.bucketName returns "proxy-bucket"
      mockProxyUploader.uploadStreamNoChecks(any,any,any,any,any) returns Success("path/to/uploaded/file_prox.mp4",1234L)

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig],fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      )

      val outputRecord = mockArchivedRecord.copy(proxyPath = Some("path/to/uploaded/file_prox.mp4"), proxyBucket = Some("proxy-bucket"))
      result must beLeft("No file exists on shape VX-456 for item VX-123 yet")
      there was no(vidispineCommunicator).streamFileContent(any, any)
      there was no(mockProxyUploader).uploadStreamNoChecks(mockedInputStream, "path/to/uploaded/file_prox.mp4", "video/mp4", Some(1234L), Some("deadbeef"))
      there was no(archiveHunterCommunicator).importProxy("abcdefg", "path/to/uploaded/file_prox.mp4", "proxy-bucket", ArchiveHunter.ProxyType.VIDEO)
      there was no(mockArchivedRecordDAO).writeRecord(any)
    }

    "return a failed Future if there is no such shape on the item" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      val mockedInputStream = mock[InputStream]

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.streamFileContent(any,any) returns Future(mockedInputStream)
      vidispineCommunicator.findItemShape(any,any) returns Future(None)
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      mockProxyUploader.bucketName returns "proxy-bucket"
      mockProxyUploader.uploadStreamNoChecks(any,any,any,any,any) returns Success("path/to/uploaded/file_prox.mp4",1234L)

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Try { Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      ) }

      result must beFailedTry
      there was no(vidispineCommunicator).streamFileContent(any, any)
      there was no(mockProxyUploader).uploadStreamNoChecks(mockedInputStream, "path/to/uploaded/file_prox.mp4", "video/mp4", Some(1234L), Some("deadbeef"))
      there was no(archiveHunterCommunicator).importProxy("abcdefg", "path/to/uploaded/file_prox.mp4", "proxy-bucket", ArchiveHunter.ProxyType.VIDEO)
      there was no(mockArchivedRecordDAO).writeRecord(any)
    }

    "not stream the file content but return Right if the shape tag is not recognised" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      val mockedInputStream = mock[InputStream]
      val sampleFile = VSShapeFile("VX-789",
        "VX-789.mp4",
        Seq("file:///path/to/Vidispine/Proxies/VX-789.mp4"),
        "CLOSED",
        1234L,
        Some("deadbeef"),
        "",
        1,
        "VX-3"
      )
      val shapeDoc = ShapeDocument(
        "VX-456",
        "",
        Some(1),
        Seq("original"),
        Seq("video/mxf"),
        Some(SimplifiedComponent("VX-111",Seq(sampleFile))),
        Some(Seq(SimplifiedComponent("VX-112",Seq(sampleFile)))),
        Some(Seq(SimplifiedComponent("VX-113",Seq(sampleFile))))
      )

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.streamFileContent(any,any) returns Future(mockedInputStream)
      vidispineCommunicator.findItemShape(any,any) returns Future(Some(shapeDoc))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      mockProxyUploader.bucketName returns "proxy-bucket"
      mockProxyUploader.uploadStreamNoChecks(any,any,any,any,any) returns Success("path/to/uploaded/file_prox.mp4",1234L)

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Try {
        Await.result(
          toTest.uploadShapeIfRequired("VX-123", "VX-456", "original", mockArchivedRecord),
          10.seconds
        )
      }

      result must beAFailedTry(SilentDropMessage())
      there was no(vidispineCommunicator).streamFileContent(any,any)
      there was no(mockProxyUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was no(mockMediaUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was no(archiveHunterCommunicator).importProxy("abcdefg", "path/to/uploaded/file_prox.mp4", "proxy-bucket", ArchiveHunter.ProxyType.VIDEO)
      there was no(mockArchivedRecordDAO).writeRecord(any)
    }

    "return Left if the upload fails" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      val mockedInputStream = mock[InputStream]
      val sampleFile = VSShapeFile("VX-789",
        "VX-789.mp4",
        Seq("file:///path/to/Vidispine/Proxies/VX-789.mp4"),
        "CLOSED",
        1234L,
        Some("deadbeef"),
        "",
        1,
        "VX-3"
      )
      val shapeDoc = ShapeDocument(
        "VX-456",
        "",
        Some(1),
        Seq("lowres"),
        Seq("video/mp4"),
        Some(SimplifiedComponent("VX-111",Seq(sampleFile))),
        Some(Seq(SimplifiedComponent("VX-112",Seq(sampleFile)))),
        Some(Seq(SimplifiedComponent("VX-113",Seq(sampleFile))))
      )

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.streamFileContent(any,any) returns Future(mockedInputStream)
      vidispineCommunicator.findItemShape(any,any) returns Future(Some(shapeDoc))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      mockProxyUploader.bucketName returns "proxy-bucket"
      mockProxyUploader.uploadStreamNoChecks(any,any,any,any,any) throws new RuntimeException("kaboom")

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      )

      result must beLeft("Could not upload List(file:///path/to/Vidispine/Proxies/VX-789.mp4) to S3")
      there was no(mockProxyUploader).copyFileToS3(any,any)
      there was no(mockMediaUploader).uploadStreamNoChecks(any,any,any,any,any)
      there was no(archiveHunterCommunicator).importProxy(any,any,any,any)
      there was no(mockArchivedRecordDAO).writeRecord(any)
    }

  }

  "VidispineMessageProcessor.getRelativePath" should {
    "make a path relative to the media assets if maybeImportSource is not set" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(PlutoCoreConfig(
        "https://fake-uri",
        "notasecret",
        Paths.get("/media/root")
      ), fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      toTest.getRelativePath("/media/root/working_group/production/footage/something.mxf", None) must beRight(Paths.get("working_group/production/footage/something.mxf"))
    }

    "make a path relative to the media assets if maybeImportSource contains another source" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(PlutoCoreConfig(
        "https://fake-uri",
        "notasecret",
        Paths.get("/media/root")
      ), fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      toTest.getRelativePath("/media/root/working_group/production/footage/something.mxf", Some("hfsdghsdf")) must beRight(Paths.get("working_group/production/footage/something.mxf"))
    }

    "make a path relative to the deliverables if maybeImportSource contains 'pluto-deliverables'" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(PlutoCoreConfig(
        "https://fake-uri",
        "notasecret",
        Paths.get("/media/root")
      ), fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      toTest.getRelativePath("/media/root/working_group/production/footage/something.mxf", Some("pluto-deliverables")) must beRight(Paths.get("Deliverables/something.mxf"))
    }
  }

  "VidispineMessageProcessor.handleMetadataUpdate" should {
    "Fail Future when there is no itemId" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]

      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(None)
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]

      val mockMsg = mock[Json]
      mockMsg.noSpaces returns "{ 'key': 'value' }"

      val mockMediaIngested = mock[VidispineMediaIngested]
      mockMediaIngested.itemId returns None

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Try { Await.result(
        toTest.handleMetadataUpdate(mockMsg, mockMediaIngested),
        10.seconds
        )
      }

      there was no(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      result must beFailedTry
    }

    "Silent ignore message when there is an IgnoreRecord for the itemId" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val mockArchivedRecord = mock[ArchivedRecord]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val mockIgnoredRecord = mock[IgnoredRecord]
      mockIgnoredRecord.ignoreReason returns "Top secret!"
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(Some(mockIgnoredRecord))
      implicit val failureRecordDAO = mock[FailureRecordDAO]

      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(None)
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]

      val mockMsg = mock[Json]
      mockMsg.noSpaces returns "{ 'key': 'value' }"

      val mockMediaIngested = mock[VidispineMediaIngested]
      mockMediaIngested.itemId returns Some("VX-123")

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Try { Await.result(
          toTest.handleMetadataUpdate(mockMsg, mockMediaIngested),
          10.seconds
        )
      }

      there was no(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      result must beAFailedTry(SilentDropMessage())
    }

    "return Left when there are no records of the itemId" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val mockArchivedRecord = mock[ArchivedRecord]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val mockIgnoredRecord = mock[IgnoredRecord]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]

      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(None)
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]

      val mockMsg = mock[Json]
      mockMsg.noSpaces returns "{ 'key': 'value' }"

      val mockMediaIngested = mock[VidispineMediaIngested]
      mockMediaIngested.itemId returns Some("VX-123")

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.handleMetadataUpdate(mockMsg, mockMediaIngested),
        10.seconds
      )

      there was no(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      result must beLeft("No record of vidispine item VX-123")
    }

    "return Left when the archivedRecord is not validated" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val mockArchivedRecord = mock[ArchivedRecord]
      mockArchivedRecord.archiveHunterIDValidated returns false
      mockArchivedRecord.originalFilePath returns "original/file/path"
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val mockIgnoredRecord = mock[IgnoredRecord]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]

      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(None)
      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]

      val mockMsg = mock[Json]
      mockMsg.noSpaces returns "{ 'key': 'value' }"

      val mockMediaIngested = mock[VidispineMediaIngested]
      mockMediaIngested.itemId returns Some("VX-123")

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.handleMetadataUpdate(mockMsg, mockMediaIngested),
        10.seconds
      )

      there was no(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      result must beLeft("ArchiveHunter ID for original/file/path has not been validated yet")
    }

    "return updated archivedRecord when metadata has been uploaded" in {
      val mockArchivedRecord =
        ArchivedRecord("archive hunter ID", "/path/to/original/file", 123456L, "some-bucket", "/path/to/uploaded/file.mp4", None)
          .copy(archiveHunterIDValidated = true)

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.writeRecord(any) returns Future(123)
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val mockIgnoredRecord = mock[IgnoredRecord]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]

      val mockHttpEntity = mock[HttpEntity]
      val mockDataBytes = mock[Source[ByteString, Any]]
      val mockContentType = mock[ContentType]
      val mockContentLengthOption = mock[Option[Long]]
      mockHttpEntity.dataBytes returns mockDataBytes
      mockHttpEntity.contentType returns mockContentType
      mockHttpEntity.contentLengthOption returns mockContentLengthOption

      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(Some(mockHttpEntity))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )

      val mockMediaUploader = mock[FileUploader]
      val mockProxyUploader = mock[FileUploader]
      val mockUploadResponse = mock[MultipartUploadResult]
      mockUploadResponse.key returns "/uploaded/file/path"
      mockUploadResponse.bucket returns "proxyBucket"

      mockProxyUploader.uploadAkkaStream(any, any, any, any ,any, any)(any, any) returns Future(mockUploadResponse)

      val mockMsg = mock[Json]
      mockMsg.noSpaces returns "{ 'key': 'value' }"

      val mockMediaIngested = mock[VidispineMediaIngested]
      mockMediaIngested.itemId returns Some("VX-123")
      mockMediaIngested.essenceVersion returns Some(2)

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.handleMetadataUpdate(mockMsg, mockMediaIngested),
        10.seconds
      )

      there was one(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      result must beRight
      result.right.get.content mustEqual (mockArchivedRecord
        .copy(
          proxyBucket = Some("proxyBucket"),
          metadataXML = Some("/uploaded/file/path"),
          metadataVersion = Some(2)
        ).asJson)
      }
    }

    "VidispineMessageProcessor.uploadMetadataToS3" should {
      "fail Future when there is no metadata for the Vidispine itemId" in {
        val mockArchivedRecord = mock[ArchivedRecord]
        implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
        mockArchivedRecordDAO.writeRecord(any) returns Future(123)
        implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
        implicit val failureRecordDAO = mock[FailureRecordDAO]

        implicit val vsCommunicator = mock[VidispineCommunicator]
        vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(None)
        val mockMediaUploader = mock[FileUploader]
        val mockProxyUploader = mock[FileUploader]
        implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
        archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
        implicit val actorSystem = mock[ActorSystem]
        implicit val materializer = mock[Materializer]

        val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

        val result = Try { Await.result(
          toTest.uploadMetadataToS3("VX-123", Some(1), mockArchivedRecord),
          10.seconds
        )}

        there was no(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
        there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
        result must beFailedTry
      }

      "return Updated ArchiveRecord when metadata has been uploaded successfully" in {
        val mockArchivedRecord =
          ArchivedRecord("archive hunter ID", "/path/to/original/file", 123456L, "some-bucket", "/path/to/uploaded/file.mp4", None)
            .copy(archiveHunterIDValidated = true)

        implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
        mockArchivedRecordDAO.writeRecord(any) returns Future(123)
        implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
        implicit val failureRecordDAO = mock[FailureRecordDAO]

        val mockHttpEntity = mock[HttpEntity]
        val mockDataBytes = mock[Source[ByteString, Any]]
        val mockContentType = mock[ContentType]
        val mockContentLengthOption = mock[Option[Long]]
        mockHttpEntity.dataBytes returns mockDataBytes
        mockHttpEntity.contentType returns mockContentType
        mockHttpEntity.contentLengthOption returns mockContentLengthOption

        implicit val actorSystem = mock[ActorSystem]
        implicit val mat = mock[Materializer]
        implicit val vsCommunicator = mock[VidispineCommunicator]
        vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(Some(mockHttpEntity))
        implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
        archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )

        val mockMediaUploader = mock[FileUploader]
        val mockProxyUploader = mock[FileUploader]
        val mockUploadResponse = mock[MultipartUploadResult]
        mockUploadResponse.key returns "/uploaded/file/path"
        mockUploadResponse.bucket returns "proxyBucket"

        mockProxyUploader.uploadAkkaStream(any, any, any, any ,any, any)(any, any) returns Future(mockUploadResponse)

        val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

        val result = Await.result(
          toTest.uploadMetadataToS3("VX-123", Some(1), mockArchivedRecord),
          10.seconds
        )

        there was one(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
        there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
        result must beRight
        result.right.get.content mustEqual (mockArchivedRecord
          .copy(
            proxyBucket = Some("proxyBucket"),
            metadataXML = Some("/uploaded/file/path"),
            metadataVersion = Some(1)
          ).asJson)
      }

      "return Left error and write FailureRecord in case of a crash" in {
        val mockArchivedRecord = mock[ArchivedRecord]
        mockArchivedRecord.originalFilePath returns "original/path"

        implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
        mockArchivedRecordDAO.writeRecord(any) returns Future(123)
        implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
        implicit val failureRecordDAO = mock[FailureRecordDAO]
        failureRecordDAO.writeRecord(any) returns Future(123)

        val mockHttpEntity = mock[HttpEntity]
        val mockDataBytes = mock[Source[ByteString, Any]]
        val mockContentType = mock[ContentType]
        val mockContentLengthOption = mock[Option[Long]]
        mockHttpEntity.dataBytes returns mockDataBytes
        mockHttpEntity.contentType returns mockContentType
        mockHttpEntity.contentLengthOption returns mockContentLengthOption

        implicit val actorSystem = mock[ActorSystem]
        implicit val mat = mock[Materializer]
        implicit val vsCommunicator = mock[VidispineCommunicator]
        vsCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(Some(mockHttpEntity))
        implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
        archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )

        val mockMediaUploader = mock[FileUploader]
        val mockProxyUploader = mock[FileUploader]
        val mockUploadResponse = mock[MultipartUploadResult]
        mockUploadResponse.key returns "/uploaded/file/path"
        mockUploadResponse.bucket returns "proxyBucket"

        mockProxyUploader.uploadAkkaStream(any, any, any, any ,any, any)(any, any) returns Future.failed(new NullPointerException("Big" +
          " bang!"))

        val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig, mockMediaUploader, mockProxyUploader)

        val result = Await.result(
          toTest.uploadMetadataToS3("VX-123", Some(1), mockArchivedRecord),
          10.seconds
        )

        val failedRec = FailureRecord(id = None,
          originalFilePath = "original/path",
          attempt = 1,
          errorMessage = "Big bang!",
          errorComponent = ErrorComponents.AWS,
          retryState = RetryStates.WillRetry)

        result must beLeft("Big bang!")
        there was one(failureRecordDAO).writeRecord(failedRec)
        there was one(mockProxyUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
        there was no(mockMediaUploader).uploadAkkaStream(any, any, any, any ,any, any)(any, any)
      }
    }
}
