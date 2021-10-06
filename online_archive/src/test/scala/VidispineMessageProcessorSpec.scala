import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecordDAO, IgnoredRecord, IgnoredRecordDAO}
import io.circe.syntax.EncoderOps
import io.circe.Json
import io.circe.generic.auto._
import messages.{VidispineField, VidispineMediaIngested}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import plutocore.PlutoCoreConfig

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.util.Try

class VidispineMessageProcessorSpec extends Specification with Mockito {
  "VidispineMessageProcessor.handleIngestedMediaInArchive" should {
    "use the full path for upload if it can't relativize" in {
      implicit val archivedRecordDAO: ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO: IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val uploader: FileUploader = mock[FileUploader]

      val basePath = Paths.get("/dummy/base/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server", "notsecret", basePath))

      toTest.getRelativePath("/some/totally/other/path") must beLeft()
    }
  }

  "VidispineMessageProcessor.handleIngestedMedia" should {
    "fail request when job status includes FAIL" in {
      implicit val archivedRecordDAO: ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO: IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val uploader: FileUploader = mock[FileUploader]
      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val basePath = Paths.get("/dummy/base/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server", "notsecret", basePath))

      val result = Try {
        Await.result(toTest.handleIngestedMedia(mediaIngested), 3.seconds)
      }

      result must beFailedTry
    }
  }

  "VidispineMessageProcessor.uploadIfRequiredAndNotExists" should {
    "ignore message if ignoreRecord exists" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      val record = IgnoredRecord(Some(1), "/original/file/path/video.mp4", "supersecret", None, None)
      ignoredRecordDAO.findBySourceFilename(any) returns Future(Some(record))

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      val basePath = Paths.get("/media/assets")
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.objectExists(any) returns Try(true)
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val fields: List[VidispineField] = List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-456=/original/file/path/video.mp4")
      )
      val ingested = VidispineMediaIngested(fields)

      val result = Await.result(toTest.uploadIfRequiredAndNotExists("/original/file/path/video.mp4", "/original", ingested), 2.seconds)

      result mustEqual Left("Record should be ignored")
    }

    "retry if there is no Archive hunter ID yet" in {
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
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.objectExists(any) returns Try(true)
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val fields: List[VidispineField] = List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-456=/original/file/path/video.mp4")
      )
      val ingested = VidispineMediaIngested(fields)

      val record = ArchivedRecord(
        id=Some(1),
        archiveHunterID="archiveId",
        archiveHunterIDValidated=false,
        originalFilePath="/original/file/path/video.mp4",
        originalFileSize=100,
        uploadedBucket="bucket",
        uploadedPath="/file/path/video.mp4",
        uploadedVersion=None,
        vidispineItemId=None,
        vidispineVersionId=None,
        proxyBucket=None,
        proxyPath=None,
        proxyVersion=None,
        metadataXML=None,
        metadataVersion=None
      )
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(record))

      val result = Await.result(toTest.uploadIfRequiredAndNotExists("/original/file/path/video.mp4", "/original", ingested), 2.seconds)

      result mustEqual Left("Archive hunter ID does not exist yet, will retry")
    }

    "return archiveRecord if file already exists in s3" in {
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
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.objectExists(any) returns Try(true)
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val fields: List[VidispineField] = List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-456=/original/file/path/video.mp4")
      )
      val ingested = VidispineMediaIngested(fields)

      val record = ArchivedRecord(
        id=Some(1),
        archiveHunterID="archiveId",
        archiveHunterIDValidated=true,
        originalFilePath="/original/file/path/video.mp4",
        originalFileSize=100,
        uploadedBucket="bucket",
        uploadedPath="/file/path/video.mp4",
        uploadedVersion=None,
        vidispineItemId=None,
        vidispineVersionId=None,
        proxyBucket=None,
        proxyPath=None,
        proxyVersion=None,
        metadataXML=None,
        metadataVersion=None
      )
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(record))

      val result = Await.result(toTest.uploadIfRequiredAndNotExists("/original/file/path/video.mp4", "/original", ingested), 2.seconds)

      result mustEqual Right(record.asJson)
    }

    "upload and update archive record if record exist and file doesn't already exist in s3" in {
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
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.objectExists(any) returns Try(false)
      uploader.copyFileToS3(any, any) returns Try(("/file/path/video.mp4", 100))
      uploader.bucketName returns "bucket"
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val fields: List[VidispineField] = List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-456=/original/file/path/video.mp4")
      )
      val ingested = VidispineMediaIngested(fields)

      val record = ArchivedRecord(
        id=Some(123),
        archiveHunterID="archiveId",
        archiveHunterIDValidated=true,
        originalFilePath="/original/file/path/video.mp4",
        originalFileSize=80,
        uploadedBucket="bucket",
        uploadedPath="OLD/file/path/video.mp4",
        uploadedVersion=None,
        vidispineItemId=None,
        vidispineVersionId=None,
        proxyBucket=None,
        proxyPath=None,
        proxyVersion=None,
        metadataXML=None,
        metadataVersion=None
      )
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(record))

      val result = Await.result(toTest.uploadIfRequiredAndNotExists("/original/file/path/video.mp4", "/original", ingested), 4.seconds)

      val recordCopy = record.copy(
        originalFileSize = 100,
        uploadedPath = "/file/path/video.mp4",
      )

      result mustEqual Right(recordCopy.asJson)
    }

    "upload file and create new archive record if archive doesn't already exit" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      ignoredRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      val basePath = Paths.get("/media/assets")
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.objectExists(any) returns Try(false)
      uploader.copyFileToS3(any, any) returns Try(("/file/path/video.mp4", 100))
      uploader.bucketName returns "bucket"
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val fields: List[VidispineField] = List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-456=/original/file/path/video.mp4")
      )
      val ingested = VidispineMediaIngested(fields)

      val record = ArchivedRecord(
        id=Some(123),
        archiveHunterID=utils.ArchiveHunter.makeDocId("bucket", "/file/path/video.mp4"),
        archiveHunterIDValidated=false,
        originalFilePath="/original/file/path/video.mp4",
        originalFileSize=100,
        uploadedBucket="bucket",
        uploadedPath="/file/path/video.mp4",
        uploadedVersion=None,
        vidispineItemId=None,
        vidispineVersionId=None,
        proxyBucket=None,
        proxyPath=None,
        proxyVersion=None,
        metadataXML=None,
        metadataVersion=None
      )

      val result = Await.result(toTest.uploadIfRequiredAndNotExists("/original/file/path/video.mp4", "/original", ingested), 4.seconds)

      result mustEqual Right(record.asJson)
    }
  }
}
