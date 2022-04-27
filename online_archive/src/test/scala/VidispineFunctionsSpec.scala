import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.ArchiveHunterCommunicator
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.{VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{FileDocument, ShapeDocument, SimplifiedComponent, VSShapeFile, VidispineCommunicator}
import io.circe.syntax._
import io.circe.generic.auto._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import utils.ArchiveHunter

import java.io.InputStream
import java.nio.file.{Path, Paths}
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class VidispineFunctionsSpec extends Specification with Mockito {
  "VidispineFunctions.uploadIfRequiredAndNotExists" should {
    "return a success message without an upload attempt but setting the vidispine item id" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockVSFunctions = mock[VidispineFunctions]
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

      val toTest = new VidispineFunctions(mockMediaUploader, mockProxyUploader)
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
      there was no(mockProxyUploader).objectExists(any)
      there was no(mockProxyUploader).copyFileToS3(any,any)
      there was one(archivedRecordDAO).writeRecord(argThat((rec:ArchivedRecord)=>rec.vidispineItemId.contains("VX-123")))
      there was no(ignoredRecordDAO).writeRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
      result must beRight
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
          Some(Seq("file:///srv/proxies/another/location/for/proxies/VX-1234.mp4")),
          "CLOSED",
          1234L,
          Some("deadbeef"),
          "2021-01-02T03:04:05.678Z",
          1,
          "VX-2"
        )
        VidispineFunctions.uploadKeyForProxy(testArchivedRecord, testProxy) mustEqual "path/to/uploaded/file_prox.mp4"
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
          None,
          "CLOSED",
          1234L,
          Some("deadbeef"),
          "2021-01-02T03:04:05.678Z",
          1,
          "VX-2"
        )
        VidispineFunctions.uploadKeyForProxy(testArchivedRecord, testProxy) mustEqual "path/to/uploaded/file_prox"
      }
    }
  }

  "VidispineFunctions.uploadShapeIfRequired" should {
    "stream the file content, tell ArchiveHunter to update its proxy and then update the datastore record" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      val mockedInputStream = mock[InputStream]
      val sampleFile = VSShapeFile("VX-789",
        "VX-789.mp4",
        Some(Seq("file:///path/to/Vidispine/Proxies/VX-789.mp4")),
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
        Some(Seq("video/mp4")),
        Some(SimplifiedComponent("VX-111",Seq(sampleFile))),
        Some(Seq(SimplifiedComponent("VX-112",Seq(sampleFile)))),
        Some(Seq(SimplifiedComponent("VX-113",Seq(sampleFile)))),
        None
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
      mockProxyUploader.copyFileToS3(any,any) returns Success("path/to/uploaded/file_prox.mp4", 1234L)
      val toTest = new VidispineFunctions(mockMediaUploader, mockProxyUploader) {
        override protected def internalCheckFile(filePath: Path): Boolean = true
      }

      val result = Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      )

      val outputRecord = mockArchivedRecord.copy(proxyPath = Some("path/to/uploaded/file_prox.mp4"), proxyBucket = Some("proxy-bucket"))
      result must beRight(MessageProcessorReturnValue(outputRecord.asJson))
      there was no(vidispineCommunicator).streamFileContent(any,any)
      there was one(mockProxyUploader).copyFileToS3(any, org.mockito.ArgumentMatchers.eq(Some("path/to/uploaded/file_prox.mp4")))
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
        Some(Seq("video/mp4")),
        Some(SimplifiedComponent("VX-111",Seq())),
        Some(Seq(SimplifiedComponent("VX-112",Seq()))),
        Some(Seq(SimplifiedComponent("VX-113",Seq()))),
        None
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

      val toTest = new VidispineFunctions(mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      )

      result must beLeft("No file exists on shape VX-456 for item VX-123 yet")
      there was no(vidispineCommunicator).streamFileContent(any, any)
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
      val toTest = new VidispineFunctions(mockMediaUploader, mockProxyUploader)

      val result = Try { Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      ) }

      result must beFailedTry
      there was no(vidispineCommunicator).streamFileContent(any, any)
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
        Some(Seq("file:///path/to/Vidispine/Proxies/VX-789.mp4")),
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
        Some(Seq("video/mxf")),
        Some(SimplifiedComponent("VX-111",Seq(sampleFile))),
        Some(Seq(SimplifiedComponent("VX-112",Seq(sampleFile)))),
        Some(Seq(SimplifiedComponent("VX-113",Seq(sampleFile)))),
        None
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
      val toTest = new VidispineFunctions(mockMediaUploader, mockProxyUploader)

      val result = Try {
        Await.result(
          toTest.uploadShapeIfRequired("VX-123", "VX-456", "original", mockArchivedRecord),
          10.seconds
        )
      }

      result must beAFailedTry(SilentDropMessage())
      there was no(vidispineCommunicator).streamFileContent(any,any)
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
        Some(Seq("file:///path/to/Vidispine/Proxies/VX-789.mp4")),
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
        Some(Seq("video/mp4")),
        Some(SimplifiedComponent("VX-111",Seq(sampleFile))),
        Some(Seq(SimplifiedComponent("VX-112",Seq(sampleFile)))),
        Some(Seq(SimplifiedComponent("VX-113",Seq(sampleFile)))),
        None
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
      val toTest = new VidispineFunctions(mockMediaUploader, mockProxyUploader)

      val result = Await.result(
        toTest.uploadShapeIfRequired("VX-123","VX-456","lowres", mockArchivedRecord),
        10.seconds
      )

      result must beLeft("Could not upload Some(List(file:///path/to/Vidispine/Proxies/VX-789.mp4)) to S3")
      there was no(mockProxyUploader).copyFileToS3(any,any)
      there was no(archiveHunterCommunicator).importProxy(any,any,any,any)
      there was no(mockArchivedRecordDAO).writeRecord(any)
    }

  }
}
