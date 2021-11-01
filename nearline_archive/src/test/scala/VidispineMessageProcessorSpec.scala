import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.storagetier.framework.MessageProcessorReturnValue
import com.gu.multimedia.storagetier.messages.{VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{FileDocument, VidispineCommunicator}
import com.om.mxs.client.japi.{MxsObject, Vault}
import matrixstore.MatrixStoreConfig
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.syntax._
import io.circe.generic.auto._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try

class VidispineMessageProcessorSpec extends Specification with Mockito {
  implicit val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

  "VidispineMessageProcessor.handleIngestedMedia" should {
    "fail request when job status includes FAIL" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockCopier = mock[FileCopier]

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilder]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Try {
        Await.result(toTest.handleIngestedMedia(mockVault, mediaIngested), 3.seconds)
      }

      result must beFailedTry
    }

    "call out to uploadIfRequiredAndNotExists" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2")
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = mock[MXSConnectionBuilder]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "12345"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val mockUploadIfReqd = mock[(Vault, String, VidispineMediaIngested) => Future[Either[String, MessageProcessorReturnValue]]]
      val fakeResult = mock[MessageProcessorReturnValue]
      mockUploadIfReqd.apply(any,any,any) returns Future(Right(fakeResult))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: VidispineMediaIngested)
        : Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfReqd(vault, absPath, mediaIngested)
      }

      val result = Await.result(toTest.handleIngestedMedia(mockVault, mediaIngested), 2.seconds)
      there was one(mockUploadIfReqd).apply(mockVault, "/absolute/path/relative/path.mp4", mediaIngested)
      result must beRight(fakeResult)
    }
  }

  "VidispineMessageProcessor.uploadIfRequiredAndNotExists" should {
    "return NearlineRecord when file has been copied to MatrixStore" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2")
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-123"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]
      mockCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Right("object-id"))

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = mock[MXSConnectionBuilder]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "12345"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Await.result(toTest.uploadIfRequiredAndNotExists(mockVault, "/absolute/path/to/file", mediaIngested), 2.seconds)
      result must beRight(MessageProcessorReturnValue(mockNearlineRecord.asJson))
    }

    "return Left when file copy to MatrixStore fail" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2")
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-123"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]
      mockCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Left("Something went wrong!!"))

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = mock[MXSConnectionBuilder]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "12345"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Await.result(toTest.uploadIfRequiredAndNotExists(mockVault, "/absolute/path/to/file", mediaIngested), 2.seconds)
      result must beLeft("Something went wrong!!")
    }

    "return FailureRecord when exception is thrown during file copy" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, "deadbeef", "2020-01-02T03:04:05Z", 1, "VX-2")
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-123"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]
      mockCopier.copyFileToMatrixStore(any, any, any, any) throws new RuntimeException("Crash during copy!!!")

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = mock[MXSConnectionBuilder]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "12345"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Await.result(toTest.uploadIfRequiredAndNotExists(mockVault, "/absolute/path/to/file", mediaIngested), 2.seconds)
      result must beLeft("Crash during copy!!!")
    }
  }
}
