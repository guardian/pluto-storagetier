import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.mxscopy.{MXSConnectionBuilder, MXSConnectionBuilderImpl, MXSConnectionBuilderMock}
import com.gu.multimedia.storagetier.framework.MessageProcessorReturnValue
import com.gu.multimedia.storagetier.messages.{VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{FileDocument, ShapeDocument, VSShapeFile, VidispineCommunicator}
import com.om.mxs.client.japi.{MxsObject, Vault}
import matrixstore.{CustomMXSMetadata, MatrixStoreConfig}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.syntax._
import io.circe.generic.auto._

import java.time.{ZoneId, ZonedDateTime}
import java.nio.file.{Path, Paths}
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
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

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

      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

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

      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

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

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

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

      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

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

  "VidispineMessageProcessor.handleMetadataUpdate" should {
    "load a record by vidispine id, build metadata and stream it to the appliance" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val msg = VidispineMediaIngested(List(
        VidispineField("itemId","VX-12345")
      ))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

      val customMeta = CustomMXSMetadata(
        CustomMXSMetadata.TYPE_RUSHES,
        Some("1234"),
        Some("345"),
        None,
        None,
        None,
        Some("test project"),
        Some("test commission"),
        Some("test WG"),
        None,
        None,
        None,
        None
      )
      val mockBuildMetaForXML = mock[(Vault, NearlineRecord,String)=>Future[Option[MxsMetadata]]]
      mockBuildMetaForXML.apply(any,any,any) returns Future(Some(customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty)))
      val mockStreamVidispineMeta = mock[(Vault, String, MxsMetadata)=>Future[Either[String, (String, Some[String])]]]
      mockStreamVidispineMeta.apply(any,any,any) returns Future(Right(("dest-object-id",Some("checksum-here"))))

      val toTest = new VidispineMessageProcessor() {
        override protected def buildMetaForXML(vault: Vault, rec: NearlineRecord, itemId:String, nowTime:ZonedDateTime=ZonedDateTime.now()): Future[Option[MxsMetadata]] = mockBuildMetaForXML(vault, rec, itemId)

        override protected def streamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata): Future[Either[String, (String, Some[String])]] = mockStreamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Await.result(toTest.handleMetadataUpdate(msg), 2.seconds)

      result must beRight()
      result.right.get.content.noSpaces must contain("\"metadataXMLObjectId\":\"dest-object-id\",")

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockBuildMetaForXML).apply(mockVault, mockNearlineRecord, "VX-12345")
      there was one(mockStreamVidispineMeta).apply(mockVault, "VX-12345", customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty))
    }

    "return a permanent failure if there was no metadata on the item" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val msg = VidispineMediaIngested(List(
        VidispineField("itemId","VX-12345")
      ))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

      val mockBuildMetaForXML = mock[(Vault, NearlineRecord,String)=>Future[Option[MxsMetadata]]]
      mockBuildMetaForXML.apply(any,any,any) returns Future(None)
      val mockStreamVidispineMeta = mock[(Vault, String, MxsMetadata)=>Future[Either[String, (String, Some[String])]]]
      mockStreamVidispineMeta.apply(any,any,any) returns Future(Right(("dest-object-id",Some("checksum-here"))))

      val toTest = new VidispineMessageProcessor() {
        override protected def buildMetaForXML(vault: Vault, rec: NearlineRecord, itemId:String, nowTime:ZonedDateTime=ZonedDateTime.now()): Future[Option[MxsMetadata]] = mockBuildMetaForXML(vault, rec, itemId)

        override protected def streamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata): Future[Either[String, (String, Some[String])]] = mockStreamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Try { Await.result(toTest.handleMetadataUpdate(msg), 2.seconds) }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Object object-id does not have GNM compatible metadata"

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockBuildMetaForXML).apply(mockVault, mockNearlineRecord, "VX-12345")
      there was no(mockStreamVidispineMeta).apply(any,any,any)
    }

    "return a temporary failure if the streaming operation fails" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val msg = VidispineMediaIngested(List(
        VidispineField("itemId","VX-12345")
      ))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

      val customMeta = CustomMXSMetadata(
        CustomMXSMetadata.TYPE_RUSHES,
        Some("1234"),
        Some("345"),
        None,
        None,
        None,
        Some("test project"),
        Some("test commission"),
        Some("test WG"),
        None,
        None,
        None,
        None
      )
      val mockBuildMetaForXML = mock[(Vault, NearlineRecord,String)=>Future[Option[MxsMetadata]]]
      mockBuildMetaForXML.apply(any,any,any) returns Future(Some(customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty)))
      val mockStreamVidispineMeta = mock[(Vault, String, MxsMetadata)=>Future[Either[String, (String, Some[String])]]]
      mockStreamVidispineMeta.apply(any,any,any) returns Future(Left("some problem"))

      val toTest = new VidispineMessageProcessor() {
        override protected def buildMetaForXML(vault: Vault, rec: NearlineRecord, itemId:String, nowTime:ZonedDateTime=ZonedDateTime.now()): Future[Option[MxsMetadata]] = mockBuildMetaForXML(vault, rec, itemId)

        override protected def streamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata): Future[Either[String, (String, Some[String])]] = mockStreamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Await.result(toTest.handleMetadataUpdate(msg), 2.seconds)

      result must beLeft("some problem")

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockBuildMetaForXML).apply(mockVault, mockNearlineRecord, "VX-12345")
      there was one(mockStreamVidispineMeta).apply(mockVault, "VX-12345", customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty))
    }

    "return a permanent failure if the streaming operation returns a permanent failure" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val msg = VidispineMediaIngested(List(
        VidispineField("itemId","VX-12345")
      ))

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

      val customMeta = CustomMXSMetadata(
        CustomMXSMetadata.TYPE_RUSHES,
        Some("1234"),
        Some("345"),
        None,
        None,
        None,
        Some("test project"),
        Some("test commission"),
        Some("test WG"),
        None,
        None,
        None,
        None
      )
      val mockBuildMetaForXML = mock[(Vault, NearlineRecord,String)=>Future[Option[MxsMetadata]]]
      mockBuildMetaForXML.apply(any,any,any) returns Future(Some(customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty)))
      val mockStreamVidispineMeta = mock[(Vault, String, MxsMetadata)=>Future[Either[String, (String, Some[String])]]]
      mockStreamVidispineMeta.apply(any,any,any) returns Future.failed(new RuntimeException("kaboom"))

      val toTest = new VidispineMessageProcessor() {
        override protected def buildMetaForXML(vault: Vault, rec: NearlineRecord, itemId:String, nowTime:ZonedDateTime=ZonedDateTime.now()): Future[Option[MxsMetadata]] = mockBuildMetaForXML(vault, rec, itemId)

        override protected def streamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata): Future[Either[String, (String, Some[String])]] = mockStreamVidispineMeta(vault, itemId, objectMetadata)
      }
      val result = Try { Await.result(toTest.handleMetadataUpdate(msg), 2.seconds) }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual("kaboom")

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockBuildMetaForXML).apply(mockVault, mockNearlineRecord, "VX-12345")
      there was one(mockStreamVidispineMeta).apply(mockVault, "VX-12345", customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty))
    }

  }

  "VidispineMessageProcessor.buildMetaForXML" should {
    "return an MxsMetadata object populated from the original source media and filesystem specifics" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

      val mockGetOriginalMediaMeta = mock[(Vault, String)=>Future[Option[CustomMXSMetadata]]]
      mockGetOriginalMediaMeta(any,any) returns Future(Some(CustomMXSMetadata(
        CustomMXSMetadata.TYPE_RUSHES,
        Some("12345"),
        Some("34"),
        None,
        None,
        None,
        Some("Test project"),
        Some("Test commission"),
        Some("Test WG"),
        None,
        None,
        None,
        None
      )))

      val fakeNow = ZonedDateTime.of(2021,2,3,4,5,6,0,ZoneId.of("UTC"))

      val toTest = new VidispineMessageProcessor() {
        def callBuildMeta(vault:Vault, rec:NearlineRecord, itemId:String, nowTime:ZonedDateTime) = buildMetaForXML(vault, rec, itemId, nowTime)
        override protected def getOriginalMediaMeta(vault:Vault, mediaOID:String) = mockGetOriginalMediaMeta(vault, mediaOID)
      }

      val result = Await.result(toTest.callBuildMeta(mockVault, mockNearlineRecord, "VX-12345", fakeNow), 2.seconds)
      result must beSome

      result.get.intValues mustEqual Map[String,Int]("MXFS_CREATIONDAY" -> 3, "MXFS_CATEGORY" -> 4, "MXFS_COMPATIBLE" -> 1, "MXFS_CREATIONMONTH" -> 2, "MXFS_CREATIONYEAR" -> 2021)
      result.get.longValues mustEqual Map("MXFS_MODIFICATION_TIME" -> 1612325106000L, "MXFS_CREATION_TIME" -> 1612325106000L, "MXFS_ACCESS_TIME" -> 1612325106000L)
      result.get.boolValues mustEqual Map("MXFS_INTRASH"->false, "GNM_HIDDEN_FILE"->false)
      result.get.stringValues mustEqual Map(
        "MXFS_FILENAME_UPPER" -> "/ABSOLUTE/PATH/TO/VX-12345.XML",
        "GNM_PROJECT_ID" -> "12345",
        "MXFS_PARENTOID" -> "",
        "GNM_TYPE" -> "metadata",
        "MXFS_PATH" -> "/absolute/path/to/VX-12345.XML",
        "GNM_COMMISSION_ID" -> "34",
        "MXFS_FILENAME" -> "VX-12345.xml",
        "GNM_WORKING_GROUP_NAME" -> "Test WG",
        "GNM_PROJECT_NAME" -> "Test project",
        "MXFS_MIMETYPE" -> "application/xml",
        "MXFS_DESCRIPTION" -> "Vidispine metadata for VX-12345",
        "ATT_ORIGINAL_OID"->"object-id",
        "GNM_COMMISSION_NAME" -> "Test commission",
        "MXFS_FILEEXT" -> ".xml"
      )

      there was one(mockGetOriginalMediaMeta).apply(mockVault, "object-id")
    }

    "return None if the original media does not have custom metadata on it" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)

      val mockGetOriginalMediaMeta = mock[(Vault, String)=>Future[Option[CustomMXSMetadata]]]
      mockGetOriginalMediaMeta(any,any) returns Future(None)

      val fakeNow = ZonedDateTime.of(2021,2,3,4,5,6,0,ZoneId.of("UTC"))

      val toTest = new VidispineMessageProcessor() {
        def callBuildMeta(vault:Vault, rec:NearlineRecord, itemId:String, nowTime:ZonedDateTime) = buildMetaForXML(vault, rec, itemId, nowTime)
        override protected def getOriginalMediaMeta(vault:Vault, mediaOID:String) = mockGetOriginalMediaMeta(vault, mediaOID)
      }

      val result = Await.result(toTest.callBuildMeta(mockVault, mockNearlineRecord, "VX-12345", fakeNow), 2.seconds)
      result must beNone
      there was one(mockGetOriginalMediaMeta).apply(mockVault, "object-id")
    }
  }

  "VidispineMessageProcessor.streamVidispineMeta" should {
    "call out to Copier to stream the xml content onto the appliance" in {
      val fakeContent = ByteString("xml goes here")
      val fakeContentSource = Source.single(fakeContent)

      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(Some(HttpEntity(ContentTypes.`text/xml(UTF-8)`, fakeContent.length.toLong, fakeContentSource)))
      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)
      val mockStreamCopy = mock[(Source[ByteString, Any], Vault, MxsMetadata) => Future[(String, Some[String])]]
      mockStreamCopy.apply(any, any, any) returns Future(("written-oid", Some("checksum-here")))

      val toTest = new VidispineMessageProcessor() {
        override def callStreamCopy(source: Source[ByteString, Any], vault: Vault, updatedMetadata: MxsMetadata): Future[(String, Some[String])] = mockStreamCopy(source, vault, updatedMetadata)

        def callStreamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata) = streamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Await.result(toTest.callStreamVidispineMeta(mockVault, "VX-12345", MxsMetadata.empty), 2.seconds)
      result must beRight(("written-oid", Some("checksum-here")))

      there was one(mockStreamCopy).apply(fakeContentSource, mockVault, MxsMetadata.empty.withValue("DPSP_SIZE", fakeContent.length.toLong))
    }

    "return a permanent failure if the item has no metadata" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(None)
      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)
      val mockStreamCopy = mock[(Source[ByteString, Any], Vault, MxsMetadata) => Future[(String, Some[String])]]
      mockStreamCopy.apply(any, any, any) returns Future(("written-oid", Some("checksum-here")))

      val toTest = new VidispineMessageProcessor() {
        override def callStreamCopy(source: Source[ByteString, Any], vault: Vault, updatedMetadata: MxsMetadata): Future[(String, Some[String])] = mockStreamCopy(source, vault, updatedMetadata)

        def callStreamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata) = streamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Try { Await.result(toTest.callStreamVidispineMeta(mockVault, "VX-12345", MxsMetadata.empty), 2.seconds) }
      result must beAFailedTry

      there was no(mockStreamCopy).apply(any,any,any)
    }

    "return a retryable failure if the copy fails" in {
      val fakeContent = ByteString("xml goes here")
      val fakeContentSource = Source.single(fakeContent)

      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(Some(HttpEntity(ContentTypes.`text/xml(UTF-8)`, fakeContent.length.toLong, fakeContentSource)))
      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)
      val mockStreamCopy = mock[(Source[ByteString, Any], Vault, MxsMetadata) => Future[(String, Some[String])]]
      mockStreamCopy.apply(any, any, any) returns Future.failed(new RuntimeException("kaboom"))

      val toTest = new VidispineMessageProcessor() {
        override def callStreamCopy(source: Source[ByteString, Any], vault: Vault, updatedMetadata: MxsMetadata): Future[(String, Some[String])] = mockStreamCopy(source, vault, updatedMetadata)

        def callStreamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata) = streamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Await.result(toTest.callStreamVidispineMeta(mockVault, "VX-12345", MxsMetadata.empty), 2.seconds)
      result must beLeft("Could not copy metadata for item VX-12345")

      there was one(mockStreamCopy).apply(fakeContentSource, mockVault, MxsMetadata.empty.withValue("DPSP_SIZE", fakeContent.length.toLong))
    }

    "not set DPSP_SIZE if there is no size in the HttpEntity" in {
      val fakeContent = ByteString("xml goes here")
      val fakeContentSource = Source.single(fakeContent)

      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.akkaStreamXMLMetadataDocument(any, any) returns Future(Some(HttpEntity(ContentTypes.`text/xml(UTF-8)`, fakeContentSource)))
      val mockNearlineRecord = NearlineRecord(
        Some(123),
        "object-id",
        "/absolute/path/to/file",
        Some("VX-12345"),
        None,
        None,
        None
      )
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      implicit val mockBuilder = MXSConnectionBuilderMock(mockVault)
      val mockStreamCopy = mock[(Source[ByteString, Any], Vault, MxsMetadata) => Future[(String, Some[String])]]
      mockStreamCopy.apply(any, any, any) returns Future(("written-oid", Some("checksum-here")))

      val toTest = new VidispineMessageProcessor() {
        override def callStreamCopy(source: Source[ByteString, Any], vault: Vault, updatedMetadata: MxsMetadata): Future[(String, Some[String])] = mockStreamCopy(source, vault, updatedMetadata)

        def callStreamVidispineMeta(vault: Vault, itemId: String, objectMetadata: MxsMetadata) = streamVidispineMeta(vault, itemId, objectMetadata)
      }

      val result = Await.result(toTest.callStreamVidispineMeta(mockVault, "VX-12345", MxsMetadata.empty), 2.seconds)
      result must beRight(("written-oid", Some("checksum-here")))

      there was one(mockStreamCopy).apply(any, org.mockito.ArgumentMatchers.eq(mockVault), org.mockito.ArgumentMatchers.eq(MxsMetadata.empty))
    }
  }
}
