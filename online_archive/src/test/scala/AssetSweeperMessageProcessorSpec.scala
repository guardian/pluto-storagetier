import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import io.circe.Json
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, EntryStatus, PlutoCoreConfig, ProductionOffice, ProjectRecord}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.MessageProcessorReturnValue

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import io.circe.syntax._

class AssetSweeperMessageProcessorSpec extends Specification with Mockito {
  "AssetSweeperMessageProcessor.processFileAndProject" should {
    "perform an upload and record success if the project is marked as deep-archive" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.copyFileToS3(any,any) returns Success(("uploaded/path/to/file.ext", 100))
      uploader.bucketName returns "somebucket"

      val projectRecord = ProjectRecord(
        Some(3333),
        1,
        "Test project",
        ZonedDateTime.now(),
        ZonedDateTime.now(),
        "test",
        None,
        None,
        None,
        Some(true),
        None,
        EntryStatus.InProduction,
        ProductionOffice.UK
      )
      val basePath = Paths.get("/media/assets")
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      val result = Await.result(toTest.processFileAndProject(Paths.get("/media/assets/path/to/file.ext"), Some(projectRecord)), 2.seconds)
      val expectedJson = """{"id":123,"archiveHunterID":"c29tZWJ1Y2tldDp1cGxvYWRlZC9wYXRoL3RvL2ZpbGUuZXh0","archiveHunterIDValidated":false,"originalFilePath":"/media/assets/path/to/file.ext","originalFileSize":100,"uploadedBucket":"somebucket","uploadedPath":"uploaded/path/to/file.ext","uploadedVersion":null,"vidispineItemId":null,"vidispineVersionId":null,"proxyBucket":null,"proxyPath":null,"proxyVersion":null,"metadataXML":null,"metadataVersion":null}"""
      result.map(_.content.noSpaces) must beRight(expectedJson)
      there was one(archivedRecordDAO).findBySourceFilename("/media/assets/path/to/file.ext")
      there was one(archivedRecordDAO).writeRecord(any)
      there was no(ignoredRecordDAO).writeRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
      there was one(uploader).copyFileToS3(new File("/media/assets/path/to/file.ext"),Some("path/to/file.ext"))
    }

    "use the full path for upload if it can't relativize" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.copyFileToS3(any,any) returns Success(("media/assets/path/to/file.ext", 100))
      uploader.bucketName returns "somebucket"

      val projectRecord = ProjectRecord(
        Some(3333),
        1,
        "Test project",
        ZonedDateTime.now(),
        ZonedDateTime.now(),
        "test",
        None,
        None,
        None,
        Some(true),
        None,
        EntryStatus.InProduction,
        ProductionOffice.UK
      )
      val basePath = Paths.get("/completely/random/base/path")
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      val result = Await.result(toTest.processFileAndProject(Paths.get("/media/assets/path/to/file.ext"), Some(projectRecord)), 2.seconds)
      val expectedJson =
        """{"id":123,"archiveHunterID":"c29tZWJ1Y2tldDptZWRpYS9hc3NldHMvcGF0aC90by9maWxlLmV4dA==","archiveHunterIDValidated":false,"originalFilePath":"/media/assets/path/to/file.ext","originalFileSize":100,"uploadedBucket":"somebucket","uploadedPath":"media/assets/path/to/file.ext","uploadedVersion":null,"vidispineItemId":null,"vidispineVersionId":null,"proxyBucket":null,"proxyPath":null,"proxyVersion":null,"metadataXML":null,"metadataVersion":null}""".stripMargin
      result.map(_.content.noSpaces) must beRight(expectedJson)
      there was one(archivedRecordDAO).findBySourceFilename("/media/assets/path/to/file.ext")
      there was one(archivedRecordDAO).writeRecord(any)
      there was no(ignoredRecordDAO).writeRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
      there was one(uploader).copyFileToS3(new File("/media/assets/path/to/file.ext"),Some("/media/assets/path/to/file.ext"))
    }

    "perform an upload and record success if no project could be found" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val uploader:FileUploader = mock[FileUploader]
      uploader.copyFileToS3(any,any) returns Success(("uploaded/path/to/file.ext", 100))
      uploader.bucketName returns "somebucket"

      val basePath = Paths.get("/media/assets")
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      val result = Await.result(toTest.processFileAndProject(Paths.get("/media/assets/path/to/file.ext"), None), 2.seconds)
      val expectedJson = """{"id":123,"archiveHunterID":"c29tZWJ1Y2tldDp1cGxvYWRlZC9wYXRoL3RvL2ZpbGUuZXh0","archiveHunterIDValidated":false,"originalFilePath":"/media/assets/path/to/file.ext","originalFileSize":100,"uploadedBucket":"somebucket","uploadedPath":"uploaded/path/to/file.ext","uploadedVersion":null,"vidispineItemId":null,"vidispineVersionId":null,"proxyBucket":null,"proxyPath":null,"proxyVersion":null,"metadataXML":null,"metadataVersion":null}"""
      result.map(_.content.noSpaces) must beRight(expectedJson)
      there was one(archivedRecordDAO).findBySourceFilename("/media/assets/path/to/file.ext")
      there was one(archivedRecordDAO).writeRecord(any)
      there was one(uploader).copyFileToS3(new File("/media/assets/path/to/file.ext"),Some("path/to/file.ext"))
    }

    "not perform an upload and record ignored if the project is deletable" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val uploader:FileUploader = mock[FileUploader]
      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      uploader.copyFileToS3(any,any) returns Success(("uploaded/path/to/file.ext", 100))
      uploader.bucketName returns "somebucket"

      val projectRecord = ProjectRecord(
        Some(3333),
        1,
        "Test project",
        ZonedDateTime.now(),
        ZonedDateTime.now(),
        "test",
        None,
        None,
        Some(true),
        Some(false),
        None,
        EntryStatus.InProduction,
        ProductionOffice.UK
      )
      val basePath = Paths.get("/media/assets")
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      val result = Await.result(toTest.processFileAndProject(Paths.get("/media/assets/path/to/file.ext"), Some(projectRecord)), 2.seconds)
      val expectedJson = """{"id":345,"originalFilePath":"/media/assets/path/to/file.ext","ignoreReason":"project 3333 is deletable","vidispineItemId":null,"vidispineVersionId":null}"""
      result.map(_.content.noSpaces) must beRight(expectedJson)
      there was no(archivedRecordDAO).writeRecord(any)
      there was one(ignoredRecordDAO).writeRecord(any)

      there was no(uploader).copyFileToS3(new File("/media/assets/path/to/file.ext"),Some("path/to/file.ext"))
    }

    "not perform an upload and record ignored if the project is sensitive" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val uploader:FileUploader = mock[FileUploader]
      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      uploader.copyFileToS3(any,any) returns Success(("uploaded/path/to/file.ext", 100))
      uploader.bucketName returns "somebucket"

      val projectRecord = ProjectRecord(
        Some(3333),
        1,
        "Test project",
        ZonedDateTime.now(),
        ZonedDateTime.now(),
        "test",
        None,
        None,
        None,
        Some(false),
        Some(true),
        EntryStatus.InProduction,
        ProductionOffice.UK
      )
      val basePath = Paths.get("/media/assets")
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      val result = Await.result(toTest.processFileAndProject(Paths.get("/media/assets/path/to/file.ext"), Some(projectRecord)), 2.seconds)
      val expectedJson = """{"id":345,"originalFilePath":"/media/assets/path/to/file.ext","ignoreReason":"project 3333 is sensitive","vidispineItemId":null,"vidispineVersionId":null}"""
      result.map(_.content.noSpaces) must beRight(expectedJson)
      there was no(archivedRecordDAO).writeRecord(any)
      there was one(ignoredRecordDAO).writeRecord(any)

      there was no(uploader).copyFileToS3(new File("/media/assets/path/to/file.ext"),Some("path/to/file.ext"))
    }

    "return a Left indicating retryable failure if copyFileToS3 fails" in {
      implicit val archivedRecordDAO:ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO:IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val uploader:FileUploader = mock[FileUploader]
      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      uploader.copyFileToS3(any,any) returns Failure(new RuntimeException("My hovercraft is full of eels"))
      uploader.bucketName returns "somebucket"

      val projectRecord = ProjectRecord(
        Some(3333),
        1,
        "Test project",
        ZonedDateTime.now(),
        ZonedDateTime.now(),
        "test",
        None,
        None,
        None,
        Some(true),
        None,
        EntryStatus.InProduction,
        ProductionOffice.UK
      )
      val basePath = Paths.get("/media/assets")
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath))

      val result = Await.result(toTest.processFileAndProject(Paths.get("/media/assets/path/to/file.ext"), Some(projectRecord)), 2.seconds)
      result.map(_.content.noSpaces) must beLeft("My hovercraft is full of eels")
      there was no(archivedRecordDAO).writeRecord(any)
      there was no(ignoredRecordDAO).writeRecord(any)
      there was one(failureRecordDAO).writeRecord(any)
      there was one(uploader).copyFileToS3(new File("/media/assets/path/to/file.ext"),Some("path/to/file.ext"))

    }
  }

  "AssetSweeperMessageProcessor.handleReplay" should {
    "look up the corresponding pluto-core project and call processFileAndProject" in {
      implicit val archivedRecordDAO: ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val vidispineFunctions = mock[VidispineFunctions]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO: IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val uploader: FileUploader = mock[FileUploader]

      val fakeProject = mock[ProjectRecord]
      fakeProject.deep_archive returns Some(true)
      fakeProject.deletable returns None
      fakeProject.sensitive returns None

      val mockAssetFolderLookup = mock[AssetFolderLookup]
      mockAssetFolderLookup.assetFolderProjectLookup(any) returns Future(Some(fakeProject))

      val mockProcessFile = mock[(Path, Option[ProjectRecord])=>Future[Either[String, MessageProcessorReturnValue]]]
      mockProcessFile.apply(any,any) returns Future(Right(Map("fake"->"result").asJson))
      val toTest = new AssetSweeperMessageProcessor(PlutoCoreConfig("https://pluto.base.uri", "shared-secret", Paths.get("/path/to/assets"))) {
        override protected lazy val asLookup: AssetFolderLookup = mockAssetFolderLookup

        override def processFileAndProject(fullPath: Path, maybeProject: Option[ProjectRecord]): Future[Either[String, MessageProcessorReturnValue]] = mockProcessFile(fullPath, maybeProject)
      }

      val msgContent =
        """{
          |"imported_id":"VX-1234",
          |"size":123456,
          |"ignore":false,
          |"mime_type":"application/mxf",
          |"mtime":1634654625,
          |"ctime":1634654625,
          |"atime":1634654625,
          |"owner":1234,
          |"group":2345,
          |"parent_dir":"/path/to/assets/project",
          |"filename":"somefile.mxf"
          |}""".stripMargin

      val msg = io.circe.parser.parse(msgContent)
      val result = Try {
        Await.result(toTest.handleMessage("assetsweeper.replay.file", msg.right.get), 2.seconds)
      }

      result must beASuccessfulTry
      result.get must beRight
      there was one(mockAssetFolderLookup).assetFolderProjectLookup(Paths.get("/path/to/assets/project/somefile.mxf"))
      there was one(mockProcessFile).apply(Paths.get("/path/to/assets/project/somefile.mxf"), Some(fakeProject))
    }
  }
}
