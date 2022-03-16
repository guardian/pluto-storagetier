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
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, AssetFolderRecord, EntryStatus, PlutoCoreConfig, ProductionOffice, ProjectRecord}
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

  "VidispineMessageProcessor.getRelativePath" should {
    "use the full path for upload if it can't relativize" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]

      implicit val mockVSFunctions = mock[VidispineFunctions]
      implicit val archivedRecordDAO: ArchivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val ignoredRecordDAO: IgnoredRecordDAO = mock[IgnoredRecordDAO]
      ignoredRecordDAO.writeRecord(any) returns Future(345)
      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]

      val basePath = Paths.get("/dummy/base/path")
      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://pluto-core","notasecret", basePath), fakeDeliverablesConfig)

      toTest.getRelativePath("/some/totally/other/path", None) must beLeft()
    }
  }

  "VidispineMessageProcessor.handleIngestedMedia" should {
    "fail request when job status includes FAIL" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockVSFunctions = mock[VidispineFunctions]
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

      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server", "notsecret", basePath), fakeDeliverablesConfig)

      val result = Try {
        Await.result(toTest.handleIngestedMedia(mediaIngested), 3.seconds)
      }

      result must beFailedTry
    }

    "call out to uploadIfRequiredAndNotExists" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Some(Seq("file:///absolute/path/relative/path.mp4")), "CLOSED", 123456L, Some("deadbeef"), "2020-01-02T03:04:05Z", 1, "VX-2", None)
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

      val fakeResult = mock[Json]
      implicit val mockVSFunctions = mock[VidispineFunctions]
      mockVSFunctions.uploadIfRequiredAndNotExists(any,any,any) returns Future(Right(fakeResult))
      val basePath = Paths.get("/absolute/path")

      val realASLookup = new AssetFolderLookup(PlutoCoreConfig("https://fake-server","notsecret",basePath))
      val mockASLookup = mock[AssetFolderLookup]
      mockASLookup.relativizeFilePath(any) answers ((args:Any)=>realASLookup.relativizeFilePath(args.asInstanceOf[Path]))
      mockASLookup.assetFolderProjectLookup(any) returns Future(Some(ProjectRecord(None,1,"test",ZonedDateTime.now(),ZonedDateTime.now(),"test",None,None,None,None,None,EntryStatus.InProduction, ProductionOffice.UK)))

      val toTest = new VidispineMessageProcessor(PlutoCoreConfig("https://fake-server","notsecret",basePath), fakeDeliverablesConfig) {
        override protected lazy val asLookup = mockASLookup
      }

      val result = Await.result(toTest.handleIngestedMedia(mediaIngested), 2.seconds)

      result must beRight(MessageProcessorReturnValue(fakeResult))
      there was one(mockVSFunctions).uploadIfRequiredAndNotExists("/absolute/path/relative/path.mp4","relative/path.mp4",mediaIngested)
      there was one(mockASLookup).assetFolderProjectLookup(Paths.get("/absolute/path/relative/path.mp4"))
    }
  }
  "VidispineMessageProcessor.handleShapeUpdate" should {
    "call out to uploadShapeIfRequired provided that the shape should be pushed" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      vidispineCommunicator.akkaStreamFirstThumbnail(any,any) returns Future(None)
      implicit val mockVSFunctions = mock[VidispineFunctions]
      mockVSFunctions.uploadShapeIfRequired(any,any,any,any) returns Future(Right(mockArchivedRecord.asJson))
      mockVSFunctions.uploadThumbnailsIfRequired(any,any,any) returns Future(Right( () ))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig)

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beRight(MessageProcessorReturnValue(mockArchivedRecord.asJson))
      there was one(mockVSFunctions).uploadShapeIfRequired("VX-123","VX-456", "lowres", mockArchivedRecord)

      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }

    "not call out to uploadShapeIfRequired and return Left if the ArchiveHunter ID is not validated yes" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = false)
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockVSFunctions = mock[VidispineFunctions]
      mockVSFunctions.uploadShapeIfRequired(any,any,any,any) returns Future(Right(mockArchivedRecord.asJson))

      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockFileUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig)

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beLeft("ArchiveHunter ID for /path/to/original/file has not been validated yet")
      there was no(mockVSFunctions).uploadShapeIfRequired("VX-123","VX-456", "lowres", mockArchivedRecord)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }

    "not call out to uploadShapeIfRequired and return Right if the file should be ignored" in {
      val mockArchivedRecord =
        ArchivedRecord("abcdefg","/path/to/original/file",123456L, "some-bucket", "path/to/uploaded/file", None)
          .copy(archiveHunterIDValidated = true)
      val mockIgnoredRecord = IgnoredRecord(None,"/path/to/original/file", "test", Some("VX-123"), None)

      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(Some(mockArchivedRecord))
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(Some(mockIgnoredRecord))
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockVSFunctions = mock[VidispineFunctions]
      mockVSFunctions.uploadShapeIfRequired(any,any,any,any) returns Future(Right(mockArchivedRecord.asJson))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]
      val mockFileUploader = mock[FileUploader]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig)

      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beRight(MessageProcessorReturnValue(mockIgnoredRecord.asJson))
      there was no(mockVSFunctions).uploadShapeIfRequired("VX-123","VX-456", "lowres", mockArchivedRecord)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }

    "not call out to uploadShapeIfRequired and return Left if the item in question is neither archived nor ignored" in {
      implicit val mockArchivedRecordDAO = mock[ArchivedRecordDAO]
      mockArchivedRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val mockIgnoredRecordDAO = mock[IgnoredRecordDAO]
      mockIgnoredRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val mockVSFunctions = mock[VidispineFunctions]
      mockVSFunctions.uploadShapeIfRequired(any,any,any,any) returns Future(Left("this should not be called"))
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      archiveHunterCommunicator.importProxy(any,any,any,any) returns Future( () )
      implicit val actorSystem = mock[ActorSystem]
      implicit val materializer = mock[Materializer]

      val toTest = new VidispineMessageProcessor(mock[PlutoCoreConfig], fakeDeliverablesConfig)
      val result = Await.result(toTest.handleShapeUpdate("VX-456","lowres","VX-123"), 3.seconds)

      result must beLeft("No record of vidispine item VX-123")
      there was no(mockVSFunctions).uploadShapeIfRequired(any,any,any,any)
      there was one(mockArchivedRecordDAO).findByVidispineId("VX-123")
      there was one(mockIgnoredRecordDAO).findByVidispineId("VX-123")
    }
  }
}
