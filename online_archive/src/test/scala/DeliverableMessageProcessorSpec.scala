import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import messages.DeliverableAssetMessage
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import plutodeliverables.PlutoDeliverablesConfig

import scala.concurrent.ExecutionContext.Implicits.global
import io.circe.generic.auto._
import io.circe.syntax._

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class DeliverableMessageProcessorSpec extends Specification with AfterAll with Mockito {
  implicit val actorSystem = ActorSystem("DeliverableMessageProcessorSpec")
  implicit val mat:Materializer = Materializer.matFromSystem

  override def afterAll() = {
    Await.ready(actorSystem.terminate(), 30.seconds)
  }

  "DeliverableMessageProcessor" should {
    "request an upload of the incoming file" in {
      val mockedUploader = mock[FileUploader]
      mockedUploader.copyFileToS3(any,any) returns Success(("path/to/uploaded-file", 100))
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)
      archivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val ignoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      val toTest = new DeliverableMessageProcessor(PlutoDeliverablesConfig("BasePath",2), mockedUploader, "somebucket") {
        override protected def validatePathName(from: Option[String]): Future[Path] = Future(Paths.get(from.get))
      }

      val msg = DeliverableAssetMessage(
        1234,
        None,
        "somefile.mxf",
        12345678L,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        21,
        1,
        None,
        None,
        None,
        Some("/path/to/deliverables/Project Name/somefile.mxf"),
        None
      )

      val result = Await.result(toTest.handleMessage("deliverables.deliverableasset.create", msg.asJson), 3.seconds)
      val expectedJson =
        """{"id":123,"archiveHunterID":"c29tZWJ1Y2tldDpwYXRoL3RvL3VwbG9hZGVkLWZpbGU=","archiveHunterIDValidated":false,"originalFilePath":"/path/to/deliverables/Project Name/somefile.mxf","originalFileSize":100,"uploadedBucket":"somebucket","uploadedPath":"path/to/uploaded-file","uploadedVersion":null,"vidispineItemId":null,"vidispineVersionId":null,"proxyBucket":null,"proxyPath":null,"proxyVersion":null,"metadataXML":null,"metadataVersion":null}""".stripMargin
      result.map(_.noSpaces) must beRight(expectedJson)
      there was one(mockedUploader).copyFileToS3(new File("/path/to/deliverables/Project Name/somefile.mxf"), Some("BasePath/Project Name/somefile.mxf"))
      there was one(archivedRecordDAO).writeRecord(any)
      there was no(ignoredRecordDAO).writeRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
    }

    "request an upload of the incoming file if a record already exists, to ensure that it is actually there" in {
      val archivedRecord = ArchivedRecord(
        Some(1234),
        "c29tZWJ1Y2tldDpwYXRoL3RvL3VwbG9hZGVkLWZpbGU=",
        true,
        "/path/to/deliverables/Project Name/somefile.mxf",
        100,
        "somebucket",
        "Deliverables/Project Name/somefile.mxf",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      val mockedUploader = mock[FileUploader]
      mockedUploader.copyFileToS3(any,any) returns Success(("path/to/uploaded-file", 100))
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(archivedRecord))
      archivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val ignoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      val toTest = new DeliverableMessageProcessor(PlutoDeliverablesConfig("BasePath",2), mockedUploader, "somebucket") {
        override protected def validatePathName(from: Option[String]): Future[Path] = Future(Paths.get(from.get))
      }

      val msg = DeliverableAssetMessage(
        1234,
        None,
        "somefile.mxf",
        12345678L,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        21,
        1,
        None,
        None,
        None,
        Some("/path/to/deliverables/Project Name/somefile.mxf"),
        None
      )

      val result = Await.result(toTest.handleMessage("deliverables.deliverableasset.create", msg.asJson), 3.seconds)
      val expectedJson =
        """{"id":123,"archiveHunterID":"c29tZWJ1Y2tldDpwYXRoL3RvL3VwbG9hZGVkLWZpbGU=","archiveHunterIDValidated":true,"originalFilePath":"/path/to/deliverables/Project Name/somefile.mxf","originalFileSize":100,"uploadedBucket":"somebucket","uploadedPath":"path/to/uploaded-file","uploadedVersion":null,"vidispineItemId":null,"vidispineVersionId":null,"proxyBucket":null,"proxyPath":null,"proxyVersion":null,"metadataXML":null,"metadataVersion":null}""".stripMargin
      result.map(_.noSpaces) must beRight(expectedJson)
      there was one(mockedUploader).copyFileToS3(new File("/path/to/deliverables/Project Name/somefile.mxf"), Some("BasePath/Project Name/somefile.mxf"))
      there was one(archivedRecordDAO).writeRecord(any)
      there was no(ignoredRecordDAO).writeRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
    }

    "write a failure record if the upload fails" in {
      val mockedUploader = mock[FileUploader]
      mockedUploader.copyFileToS3(any,any) returns Failure(new RuntimeException("kaboom"))
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)
      archivedRecordDAO.writeRecord(any) returns Future(123)

      implicit val ignoredRecordDAO = mock[IgnoredRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      val toTest = new DeliverableMessageProcessor(PlutoDeliverablesConfig("BasePath",2), mockedUploader, "somebucket") {
        override protected def validatePathName(from: Option[String]): Future[Path] = Future(Paths.get(from.get))
      }

      val msg = DeliverableAssetMessage(
        1234,
        None,
        "somefile.mxf",
        12345678L,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        21,
        1,
        None,
        None,
        None,
        Some("/path/to/deliverables/Project Name/somefile.mxf"),
        None
      )

      val result = Await.result(toTest.handleMessage("deliverables.deliverableasset.create", msg.asJson), 3.seconds)
      result.map(_.noSpaces) must beLeft("kaboom")
      there was one(mockedUploader).copyFileToS3(new File("/path/to/deliverables/Project Name/somefile.mxf"), Some("BasePath/Project Name/somefile.mxf"))
      there was no(archivedRecordDAO).writeRecord(any)
      there was no(ignoredRecordDAO).writeRecord(any)
      there was one(failureRecordDAO).writeRecord(any)
    }
  }
}
