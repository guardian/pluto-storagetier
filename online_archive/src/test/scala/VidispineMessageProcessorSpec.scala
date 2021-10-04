import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import messages.{VidispineField, VidispineMediaIngested}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import plutocore.{EntryStatus, PlutoCoreConfig, ProductionOffice, ProjectRecord}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.util.Try

class VidispineMessageProcessorSpec extends Specification with Mockito {
  "VidispineMessageProcessor.handleIngestedMediaInArchive" should {
    "return File already exists if file with same name and size has already been imported" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      val basePath = Paths.get("/media/assets")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val fields: List[VidispineField] = List(VidispineField("originalPath", "original/path"), VidispineField("itemId",
        "VX-123"), VidispineField("bytesWritten", "12345"), VidispineField("status", "FINISHED"))
      val ingested = VidispineMediaIngested(fields)

      val record = ArchivedRecord(
        archiveHunterID="archiveId",
        originalFilePath="original/file/path",
        originalFileSize=12345,
        uploadedBucket="bucket",
        uploadedPath="uploaded/path",
        uploadedVersion=Some(4)
      )
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(record))

      val result = Await.result(toTest.handleIngestedMediaInArchive("/media/file.mp4", ingested), 2.seconds)

      result mustEqual "File already exist"
    }

    "use the full path for upload if it can't relativize" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]

      val basePath = Paths.get("/dummy/base/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      toTest.getRelativePath("/some/totally/other/path") must beLeft()
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

      "return File already exists if file with same name and size has already been imported" in {
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

        val record = ArchivedRecord(
          archiveHunterID="archiveId",
          originalFilePath="the/correct/filepath/video.mp4",
          originalFileSize=12345,
          uploadedBucket="bucket",
          uploadedPath="uploaded/path",
          uploadedVersion=Some(4)
        )
        archivedRecordDAO.findBySourceFilename(any) returns Future(Some(record))

        val basePath = Paths.get("/dummy/base/path")
        val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

        val result = Await.result(toTest.handleIngestedMedia(mediaIngested), 2.seconds)

        result mustEqual Left("File already exist")
      }
    }
  }
}
