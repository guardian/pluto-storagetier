import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.mxscopy.{MXSConnectionBuilder, MXSConnectionBuilderImpl, MXSConnectionBuilderMock}
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.{VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{FileDocument, ItemResponseSimplified, MetadataValuesWrite, QueryableItem, ShapeDocument, SimplifiedComponent, VSShapeFile, VidispineCommunicator}
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import matrixstore.{CustomMXSMetadata, MatrixStoreConfig}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.syntax._
import io.circe.generic.auto._

import java.net.URI
import java.time.{ZoneId, ZonedDateTime}
import java.nio.file.{Path, Paths}
import scala.annotation.switch
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

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
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, Some("deadbeef"), "2020-01-02T03:04:05Z", 1, "VX-2", None)
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

      val mockUploadIfReqd = mock[(Vault, String, QueryableItem) => Future[Either[String, MessageProcessorReturnValue]]]
      val fakeResult = mock[MessageProcessorReturnValue]
      mockUploadIfReqd.apply(any,any,any) returns Future(Right(fakeResult))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: QueryableItem)
        : Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfReqd(vault, absPath, mediaIngested)
      }

      val result = Await.result(toTest.handleIngestedMedia(mockVault, mediaIngested), 2.seconds)
      there was one(mockUploadIfReqd).apply(mockVault, "/absolute/path/relative/path.mp4", mediaIngested)
      result must beRight(fakeResult)
    }
  }

  "VidispineMessageProcessor.uploadIfRequiredAndNotExists" should {
    "return NearlineRecord when file has been copied to MatrixStore, and set the Nearline ID in Vidispine" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, Some("deadbeef"), "2020-01-02T03:04:05Z", 1, "VX-2", None)
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockVSFile))
      mockVSCommunicator.setGroupedMetadataValue(any,any,any,any) returns Future(Some(mock[ItemResponseSimplified]))

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

      val toTest = new VidispineMessageProcessor() {
        override protected def checkFileExists(filePath: Path): Unit = ()
      }

      val result = Await.result(toTest.uploadIfRequiredAndNotExists(mockVault, "/absolute/path/to/file", mediaIngested), 2.seconds)
      result must beRight(MessageProcessorReturnValue(mockNearlineRecord.asJson))
      there was one(mockVSCommunicator).setGroupedMetadataValue("VX-123", "Asset", "gnm_nearline_id", "object-id")
    }

    "return Left when file copy to MatrixStore fail" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, Some("deadbeef"), "2020-01-02T03:04:05Z", 1, "VX-2", None)
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

      val toTest = new VidispineMessageProcessor() {
        override protected def checkFileExists(filePath: Path): Unit = ()
      }

      val result = Await.result(toTest.uploadIfRequiredAndNotExists(mockVault, "/absolute/path/to/file", mediaIngested), 2.seconds)
      result must beLeft("Something went wrong!!")
    }

    "return FailureRecord when exception is thrown during file copy" in {
      val mockVSFile = FileDocument("VX-1234","relative/path.mp4",Seq("file:///absolute/path/relative/path.mp4"), "CLOSED", 123456L, Some("deadbeef"), "2020-01-02T03:04:05Z", 1, "VX-2", None)
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

      val toTest = new VidispineMessageProcessor() {
        override protected def checkFileExists(filePath: Path): Unit = ()
      }

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


      val mockCheckForPreExisting = mock[(Vault, Path, QueryableItem, Boolean)=>Future[Option[NearlineRecord]]]

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

        override def checkForPreExistingFiles(vault: Vault, filePath: Path, mediaIngested: QueryableItem, shouldSave: Boolean): Future[Option[NearlineRecord]] = mockCheckForPreExisting(vault, filePath, mediaIngested, shouldSave)
      }

      val result = Await.result(toTest.handleMetadataUpdate(msg), 2.seconds)

      result must beRight()
      result.right.get.content.noSpaces must contain("\"metadataXMLObjectId\":\"dest-object-id\",")

      there was no(mockCheckForPreExisting).apply(any,any,any,any)
      there was no(mockVSCommunicator).listItemShapes(any)
      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockBuildMetaForXML).apply(mockVault, mockNearlineRecord, "VX-12345")
      there was one(mockStreamVidispineMeta).apply(mockVault, "VX-12345", customMeta.copy(itemType = CustomMXSMetadata.TYPE_META).toAttributes(MxsMetadata.empty))
    }

    "check for pre-existing media if there is no database record" in {
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

      val mockShape = mock[ShapeDocument]
      mockShape.tag returns Seq("original")
      mockShape.getLikelyFile returns Some(VSShapeFile("VX-8888","/path/to/some/file",None,"CLOSED",1234L,None,"2012-01-02T03:04:05Z",1, "VX-4"))
      mockVSCommunicator.listItemShapes(any) returns Future(Some(Seq(mockShape)))

      val mockCheckForPreExisting = mock[(Vault, Path, QueryableItem, Boolean)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExisting(any,any,any,any) returns Future(Some(mockNearlineRecord))
      val msg = VidispineMediaIngested(List(
        VidispineField("itemId","VX-12345")
      ))

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
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

        override def checkForPreExistingFiles(vault: Vault, filePath: Path, mediaIngested: QueryableItem, shouldSave: Boolean): Future[Option[NearlineRecord]] = mockCheckForPreExisting(vault, filePath, mediaIngested, shouldSave)
      }

      val result = Await.result(toTest.handleMetadataUpdate(msg), 2.seconds)

      result must beRight()
      result.right.get.content.noSpaces must contain("\"metadataXMLObjectId\":\"dest-object-id\",")

      there was one(mockVSCommunicator).listItemShapes("VX-12345")
      there was one(mockCheckForPreExisting).apply(mockVault,Paths.get("/path/to/some/file"),msg,false)
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

      result must beASuccessfulTry
      result.get must beLeft("Object object-id does not have GNM compatible metadata")

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

  "VidispineMessageProcessor.handleShapeUpdate" should {
    "return Left with error message when no record exists" in {
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
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockVault = mock[Vault]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Await.result(toTest.handleShapeUpdate(
        mockVault,
        mediaIngested,
        "VX-123",
        "VX-456"), 2.seconds)

      result must beLeft("No record of vidispine item VX-456 retry later")
    }

    "return updated record when Shape is successfully copied to MatrixStore" in {
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
      nearlineRecordDAO.findByVidispineId(any) returns Future(Some(mockNearlineRecord))
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]
      mockCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Left("Error copying file"))

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockVault = mock[Vault]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val mockCopyResult = mock[MessageProcessorReturnValue]
      val mockCopyShapeIfRequired = mock[(Vault, VidispineMediaIngested, String, String, NearlineRecord) =>
        Future[Either[String, MessageProcessorReturnValue]]]

      mockCopyShapeIfRequired.apply(any, any, any, any, any) returns Future(Right(mockCopyResult))

      val toTest = new VidispineMessageProcessor() {
        override def copyShapeIfRequired(vault:Vault, mediaIngested: VidispineMediaIngested, itemId: String, shapeId: String,
                                         record: NearlineRecord): Future[Either[String, MessageProcessorReturnValue]] =
          mockCopyShapeIfRequired(vault, mediaIngested, itemId, shapeId, record)
      }

      val result = Await.result(toTest.handleShapeUpdate(
        mockVault,
        mediaIngested,
        "VX-123",
        "VX-456"), 2.seconds)

      result must beRight(mockCopyResult)

      there was one(mockCopyShapeIfRequired).apply(
        mockVault,
        mediaIngested,
        "VX-456",
        "VX-123",
        mockNearlineRecord
      )
    }
  }

  "VidispineMessageProcessor.copyShapeIfRequired" should {
    "return a failed Future when item Shape can't be found on item" in {
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
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      mockVSCommunicator.findItemShape(any, any) returns Future(None)

      val mockVault = mock[Vault]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Try { Await.result(toTest.copyShapeIfRequired(
        mockVault,
        mediaIngested,
        "VX-123",
        "VX-456",
        mockNearlineRecord), 2.seconds) }

      result must beAFailedTry
    }

    "return Left when shapeDoc doesn´t contain a file yet" in {
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
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]
      mockCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Left("Error copying file"))

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockShapeDoc = mock[ShapeDocument]
      mockShapeDoc.getLikelyFile returns None

      mockVSCommunicator.findItemShape(any, any) returns Future(Some(mockShapeDoc))

      val mockVault = mock[Vault]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor()

      val result = Await.result(toTest.copyShapeIfRequired(
        mockVault,
        mediaIngested,
        "VX-123",
        "VX-456",
        mockNearlineRecord), 2.seconds)

      result must beLeft("No file exists on shape VX-456 for item VX-123 yet")
    }

    "return updated record when Shape is successfully copied to MatrixStore" in {
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
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      failureRecordDAO.findBySourceFilename(any) returns Future(None)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockCopier = mock[FileCopier]

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockShapeDoc = mock[ShapeDocument]
      val mockShapeFile = mock[VSShapeFile]
      mockShapeDoc.getLikelyFile returns Some(mockShapeFile)

      mockVSCommunicator.findItemShape(any, any) returns Future(Some(mockShapeDoc))

      val mockVault = mock[Vault]

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val mockPath = mock[Path]
      val mockGetFilePathForShape = mock[(ShapeDocument, String, String) =>  Future[Either[String, Path]]]
      mockGetFilePathForShape.apply(any, any, any) returns Future(Right(mockPath))
      val mockUploadShapeIfRequired = mock[(Vault, Path, VidispineMediaIngested, NearlineRecord, VSShapeFile) =>
        Future[Either[String, MessageProcessorReturnValue]]]
      mockUploadShapeIfRequired.apply(any, any, any, any, any) returns Future(Right(MessageProcessorReturnValue(mockNearlineRecord.copy(
        proxyObjectId = Some("Object-id"),
        vidispineItemId = mediaIngested.itemId,
        vidispineVersionId = mediaIngested.essenceVersion,
      ).asJson)))

      val toTest = new VidispineMessageProcessor() {
        override def getFilePathForShape(shapeDoc: ShapeDocument, itemId: String, shapeId: String) =
          mockGetFilePathForShape(shapeDoc, itemId, shapeId)

        override def uploadShapeIfRequired(vault: Vault, fullPath: Path, mediaIngested: VidispineMediaIngested,
                                           nearlineRecord: NearlineRecord, proxyFile:VSShapeFile) = mockUploadShapeIfRequired(vault,
          fullPath, mediaIngested, nearlineRecord, proxyFile)
      }

      val result = Await.result(toTest.copyShapeIfRequired(
        mockVault,
        mediaIngested,
        "VX-123",
        "VX-456",
        mockNearlineRecord), 2.seconds)

      result must beRight(MessageProcessorReturnValue(mockNearlineRecord.copy(
        proxyObjectId = Some("Object-id"),
        vidispineItemId = mediaIngested.itemId,
        vidispineVersionId = mediaIngested.essenceVersion,
      ).asJson))
      there was one (mockGetFilePathForShape).apply(mockShapeDoc, "VX-123", "VX-456")
      there was one (mockUploadShapeIfRequired).apply(mockVault, mockPath, mediaIngested, mockNearlineRecord, mockShapeFile)
    }
  }

  "VidispineMessageProcessor.uploadShapeIfRequired" should {
    "return Left when copy to MatrixStore fail" in {
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
      mockCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Left("Error copying file"))

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val mockVault = mock[Vault]

      val mockShapeFile = VSShapeFile(
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

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val toTest = new VidispineMessageProcessor() {
        override def buildMetadataForProxy(vault:Vault, rec:NearlineRecord): Future[Option[MxsMetadata]] = Future(Some
        (mock[MxsMetadata]))
      }
      val result = Await.result(toTest.uploadShapeIfRequired(
        mockVault,
        Paths.get("/srv/proxies/another/location/for/proxies/VX-1234.mp4"),
        mediaIngested,
        mockNearlineRecord,
        mockShapeFile), 2.seconds)

      result must beLeft("Error copying file")
    }

    "Update file with additional metadata after file has been copied" in {
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
      mockCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Right("VX-1234"))

      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]

      val mockMxsObject = mock[MxsObject]
      doNothing.when(mockMxsObject).delete
      val mockVault = mock[Vault]
      mockVault.getObject(any) returns mockMxsObject

      val mockShapeFile = VSShapeFile(
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

      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FAILED"),
        VidispineField("essenceVersion", "3"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      val mockCallUpdateMetadata = mock[(Vault, String, MxsMetadata) => Try[Unit]]
      mockCallUpdateMetadata.apply(any,any,any) returns Success( () )
      val mockUpdateParentsMetadata = mock[(Vault, String, String, String) => Try[Unit]]
      mockUpdateParentsMetadata.apply(any,any,any,any) returns Success( () )
      val mockMetadata = mock[MxsMetadata]

      val toTest = new VidispineMessageProcessor() {
        override def buildMetadataForProxy(vault:Vault, rec:NearlineRecord): Future[Option[MxsMetadata]] = Future(Some
        (mockMetadata))

        override def callUpdateMetadata(vault:Vault, objectId: String, metadata: MxsMetadata) = mockCallUpdateMetadata(vault,
          objectId, metadata)

        override def updateParentsMetadata(vault:Vault, objectId:String, fieldName:String, fieldValue:String) =
          mockUpdateParentsMetadata(vault, objectId, fieldName, fieldValue)

        override def uploadKeyForProxy(rec: NearlineRecord, file: VSShapeFile): String = "/path/video_proxy.mp4"
      }

      val result = Await.result(toTest.uploadShapeIfRequired(
        mockVault,
        Paths.get("/srv/proxies/another/location/for/proxies/VX-1234.mp4"),
        mediaIngested,
        mockNearlineRecord,
        mockShapeFile), 2.seconds)

      there was no(mockMxsObject).delete()

      there was one (mockCallUpdateMetadata).apply(mockVault, "VX-1234", mockMetadata)
      result must beRight(MessageProcessorReturnValue(mockNearlineRecord
        .copy(
          proxyObjectId = Some("VX-1234"),
          vidispineVersionId = Some(3),
          vidispineItemId = Some("VX-123")
        )
        .asJson))
    }
  }

  "VidispineMessageProcessor.getFilePathForShape" should {
    "return Left if no file could be found in the shapeDoc" in {
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
      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockShapeDoc = mock[ShapeDocument]
      mockShapeDoc.getLikelyFile returns None

      val toTest = new VidispineMessageProcessor()
      val result = Await.result(toTest.getFilePathForShape(mockShapeDoc, "VX-1", "VX-2"), 2.seconds)

      result must beLeft("No file exists on shape VX-2 for item VX-1 yet")
    }

    "find filePath for Shape if it exists" in {
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
      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockShapeFile = VSShapeFile(
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
      val mockShapeDoc = mock[ShapeDocument]
      mockShapeDoc.getLikelyFile returns Some(mockShapeFile)

      val mockInternalCheckFile = mock[(Path) =>  Boolean]
      mockInternalCheckFile.apply(any) returns true

      val toTest = new VidispineMessageProcessor() {
        override protected def internalCheckFile(filePath: Path): Boolean = mockInternalCheckFile(filePath)
      }
      val result = Await.result(toTest.getFilePathForShape(mockShapeDoc, "VX-1", "VX-1234"), 2.seconds)

      result must beRight(Paths.get("/srv/proxies/another/location/for/proxies/VX-1234.mp4"))
      there was one (mockInternalCheckFile).apply(Paths.get(URI.create("file:///srv/proxies/another/location/for/proxies/VX-1234.mp4")))
    }

    "return a Failure if filePath for Shape doesn't exist" in {
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
      implicit val mockBuilder = mock[MXSConnectionBuilder]
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      val mockShapeFile = VSShapeFile(
        "VX-1234",
        "another/location/for/proxies/VX-1234.mp4",
        Some(Seq("file:///srv/proxy/with/illegal/location/VX-1234.mp4")),
        "CLOSED",
        1234L,
        Some("deadbeef"),
        "2021-01-02T03:04:05.678Z",
        1,
        "VX-2"
      )
      val mockShapeDoc = mock[ShapeDocument]
      mockShapeDoc.getLikelyFile returns Some(mockShapeFile)

      val mockInternalCheckFile = mock[(Path) =>  Boolean]
      mockInternalCheckFile.apply(any) returns false

      val toTest = new VidispineMessageProcessor() {
        override protected def internalCheckFile(filePath: Path): Boolean = mockInternalCheckFile(filePath)
      }

      val result = Try { Await.result(toTest.getFilePathForShape(mockShapeDoc, "VX-1", "VX-1234"), 2.seconds) }

      result must beAFailedTry
      there was one (mockInternalCheckFile).apply(Paths.get(URI.create("file:///srv/proxy/with/illegal/location/VX-1234.mp4")))
    }
  }

  "VidispineMessageProcessor.handleVidispineItemNeedsBackup" should {
    "call out to uploadIfRequiredAndNotExists if the item does not exist in the database and does have appropriate metadata" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockCopier = mock[FileCopier]

      val mockedFile = FileDocument("VX-1212","media.file",Seq("file:///some/path/to/media.file"), "CLOSED",12345L, None, "", 0, "VX-7", None)
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockedFile))

      val itemShapes = Seq(
        ShapeDocument("VX-1111","",
          Some(0),
          Seq("original"),
          None,
          Some(SimplifiedComponent(
            "VX-1212",
            Seq(VSShapeFile(mockedFile.id, mockedFile.path, Some(mockedFile.uri), mockedFile.state, mockedFile.size, mockedFile.hash, mockedFile.timestamp, mockedFile.refreshFlag, mockedFile.storage))
          )),
          None,
          None),
        ShapeDocument("VX-2222","",Some(0),Seq("lowres"),None,None,None,None)
      )
      mockVSCommunicator.listItemShapes(any) returns Future(Some(itemShapes))

      val mockedMeta = mock[ItemResponseSimplified]
      mockedMeta.valuesForField(any, any) answers ((args:Array[AnyRef])=>{
        val fieldName = args.head.asInstanceOf[String]
        val maybeGroup = args(1).asInstanceOf[Option[String]]

        (fieldName: @switch) match {
          case "gnm_nearline_id"=>Seq(MetadataValuesWrite("abcdefg"))
          case "original_filename"=>Seq(MetadataValuesWrite("/path/to/some.file"))
          case _=>
            throw new RuntimeException(s"unexpected field $fieldName")
        }
      })
      mockVSCommunicator.getMetadata(any) returns Future(Some(mockedMeta))

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val mockVault = mock[Vault]

      val mockUploadIfRequiredAndNotExists = mock[(Vault, String, QueryableItem)=>Future[Either[String, MessageProcessorReturnValue]]]
      mockUploadIfRequiredAndNotExists.apply(any,any,any) returns Future(Right(MessageProcessorReturnValue(Json.fromString("{\"test\":\"ok\""))))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: QueryableItem): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfRequiredAndNotExists(vault, absPath, mediaIngested)
      }

      val result = Await.result(toTest.handleVidispineItemNeedsBackup(mockVault, "VX-12345"), 5.seconds)

      result must beRight

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockVSCommunicator).listItemShapes("VX-12345")
      there was one(mockVSCommunicator).getFileInformation("VX-1212")
      there was one(mockUploadIfRequiredAndNotExists).apply(
        org.mockito.ArgumentMatchers.eq(mockVault),
        org.mockito.ArgumentMatchers.eq("/some/path/to/media.file"),
        org.mockito.ArgumentMatchers.any()
      )
      there was one()
    }

    "call out to uploadIfRequiredAndNotExists if the item has no shapes on it" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockCopier = mock[FileCopier]

      val mockedFile = FileDocument("VX-1212","media.file",Seq("file:///some/path/to/media.file"), "CLOSED",12345L, None, "", 0, "VX-7", None)
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockedFile))

      val itemShapes = Seq()
      mockVSCommunicator.listItemShapes(any) returns Future(Some(itemShapes))

      val mockedMeta = mock[ItemResponseSimplified]
      mockedMeta.valuesForField(any, any) answers ((args:Array[AnyRef])=>{
        val fieldName = args.head.asInstanceOf[String]
        val maybeGroup = args(1).asInstanceOf[Option[String]]

        (fieldName: @switch) match {
          case "gnm_nearline_id"=>Seq()
          case _=>
            throw new RuntimeException(s"unexpected field $fieldName")
        }
      })
      mockVSCommunicator.getMetadata(any) returns Future(Some(mockedMeta))

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val mockVault = mock[Vault]

      val mockUploadIfRequiredAndNotExists = mock[(Vault, String, QueryableItem)=>Future[Either[String, MessageProcessorReturnValue]]]
      mockUploadIfRequiredAndNotExists.apply(any,any,any) returns Future(Right(MessageProcessorReturnValue(Json.fromString("{\"test\":\"ok\""))))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: QueryableItem): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfRequiredAndNotExists(vault, absPath, mediaIngested)
      }

      val result = Try { Await.result(toTest.handleVidispineItemNeedsBackup(mockVault, "VX-12345"), 5.seconds) }

      result must beFailedTry
      result.failed.get must beAnInstanceOf[SilentDropMessage]

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockVSCommunicator).listItemShapes("VX-12345")
      there was no(mockVSCommunicator).getFileInformation(any)
      there was no(mockUploadIfRequiredAndNotExists).apply(any,any,any)
    }

    "silently drop the message if the vidispine item already has a nearline id" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockCopier = mock[FileCopier]

      val mockedFile = FileDocument("VX-1212","media.file",Seq("file:///some/path/to/media.file"), "CLOSED",12345L, None, "", 0, "VX-7", None)
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockedFile))

      val itemShapes = Seq(
        ShapeDocument("VX-1111","",
          Some(0),
          Seq("original"),
          None,
          Some(SimplifiedComponent(
            "VX-1212",
            Seq(VSShapeFile(mockedFile.id, mockedFile.path, Some(mockedFile.uri), mockedFile.state, mockedFile.size, mockedFile.hash, mockedFile.timestamp, mockedFile.refreshFlag, mockedFile.storage))
          )),
          None,
          None),
        ShapeDocument("VX-2222","",Some(0),Seq("lowres"),None,None,None,None)
      )
      mockVSCommunicator.listItemShapes(any) returns Future(Some(itemShapes))

      val mockedMeta = mock[ItemResponseSimplified]
      mockedMeta.valuesForField(any, any) answers ((args:Array[AnyRef])=>{
        val fieldName = args.head.asInstanceOf[String]
        val maybeGroup = args(1).asInstanceOf[Option[String]]

        (fieldName: @switch) match {
          case "gnm_nearline_id"=>Seq(MetadataValuesWrite("abcdefg"))
          case _=>
            throw new RuntimeException(s"unexpected field $fieldName")
        }
      })
      mockVSCommunicator.getMetadata(any) returns Future(Some(mockedMeta))

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val mockVault = mock[Vault]

      val mockUploadIfRequiredAndNotExists = mock[(Vault, String, QueryableItem)=>Future[Either[String, MessageProcessorReturnValue]]]
      mockUploadIfRequiredAndNotExists.apply(any,any,any) returns Future(Right(MessageProcessorReturnValue(Json.fromString("{\"test\":\"ok\""))))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: QueryableItem): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfRequiredAndNotExists(vault, absPath, mediaIngested)
      }

      val result = Try { Await.result(toTest.handleVidispineItemNeedsBackup(mockVault, "VX-12345"), 5.seconds) }

      //testing the exception this way feels nasty but i can't work out a better way of doing it
      result must beAFailedTry
      result.failed.get.getClass.toString mustEqual "class com.gu.multimedia.storagetier.framework.SilentDropMessage"

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockVSCommunicator).listItemShapes("VX-12345")
      there was no(mockVSCommunicator).getFileInformation(any)
      there was no(mockUploadIfRequiredAndNotExists).apply(any,any,any)
    }

    "return a retryable failure if no metadata is available" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockCopier = mock[FileCopier]

      val mockedFile = FileDocument("VX-1212","media.file",Seq("file:///some/path/to/media.file"), "CLOSED",12345L, None, "", 0, "VX-7", None)
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockedFile))

      val itemShapes = Seq(
        ShapeDocument("VX-1111","",
          Some(0),
          Seq("original"),
          None,
          Some(SimplifiedComponent(
            "VX-1212",
            Seq(VSShapeFile(mockedFile.id, mockedFile.path, Some(mockedFile.uri), mockedFile.state, mockedFile.size, mockedFile.hash, mockedFile.timestamp, mockedFile.refreshFlag, mockedFile.storage))
          )),
          None,
          None),
        ShapeDocument("VX-2222","",Some(0),Seq("lowres"),None,None,None,None)
      )
      mockVSCommunicator.listItemShapes(any) returns Future(Some(itemShapes))

      mockVSCommunicator.getMetadata(any) returns Future(None)

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val mockVault = mock[Vault]

      val mockUploadIfRequiredAndNotExists = mock[(Vault, String, QueryableItem)=>Future[Either[String, MessageProcessorReturnValue]]]
      mockUploadIfRequiredAndNotExists.apply(any,any,any) returns Future(Right(MessageProcessorReturnValue(Json.fromString("{\"test\":\"ok\""))))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: QueryableItem): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfRequiredAndNotExists(vault, absPath, mediaIngested)
      }

      val result = Await.result(toTest.handleVidispineItemNeedsBackup(mockVault, "VX-12345"), 5.seconds)

      result must beLeft

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockVSCommunicator).listItemShapes("VX-12345")
      there was one(mockVSCommunicator).getFileInformation("VX-1212")
      there was no(mockUploadIfRequiredAndNotExists).apply(any,any,any)
    }


    "return a retryable failure if the file path cannot be determined" in {
      implicit val mockVSCommunicator = mock[VidispineCommunicator]
      implicit val mockCopier = mock[FileCopier]

      val mockedFile = FileDocument("VX-1212","media.file",Seq("file:///some/path/to/media.file"), "CLOSED",12345L, None, "", 0, "VX-7", None)
      mockVSCommunicator.getFileInformation(any) returns Future(Some(mockedFile))

      val itemShapes = Seq(
        ShapeDocument("VX-1111","",
          Some(0),
          Seq("original"),
          None,
          Some(SimplifiedComponent(
            "VX-1212",
            Seq()
          )),
          None,
          None),
        ShapeDocument("VX-2222","",Some(0),Seq("lowres"),None,None,None,None)
      )
      mockVSCommunicator.listItemShapes(any) returns Future(Some(itemShapes))

      val mockedMeta = mock[ItemResponseSimplified]
      mockedMeta.valuesForField(any, any) answers ((args:Array[AnyRef])=>{
        val fieldName = args.head.asInstanceOf[String]
        val maybeGroup = args(1).asInstanceOf[Option[String]]

        (fieldName: @switch) match {
          case "gnm_nearline_id"=>Seq()
          case _=>
            throw new RuntimeException(s"unexpected field $fieldName")
        }
      })
      mockVSCommunicator.getMetadata(any) returns Future(Some(mockedMeta))

      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.findByVidispineId(any) returns Future(None)
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat: Materializer = mock[Materializer]
      implicit val sys: ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val mockVault = mock[Vault]

      val mockUploadIfRequiredAndNotExists = mock[(Vault, String, QueryableItem)=>Future[Either[String, MessageProcessorReturnValue]]]
      mockUploadIfRequiredAndNotExists.apply(any,any,any) returns Future(Right(MessageProcessorReturnValue(Json.fromString("{\"test\":\"ok\""))))

      val toTest = new VidispineMessageProcessor() {
        override def uploadIfRequiredAndNotExists(vault: Vault, absPath: String, mediaIngested: QueryableItem): Future[Either[String, MessageProcessorReturnValue]] = mockUploadIfRequiredAndNotExists(vault, absPath, mediaIngested)
      }

      val result = Await.result(toTest.handleVidispineItemNeedsBackup(mockVault, "VX-12345"), 5.seconds)

      result must beLeft

      there was one(nearlineRecordDAO).findByVidispineId("VX-12345")
      there was one(mockVSCommunicator).listItemShapes("VX-12345")
      there was no(mockVSCommunicator).getFileInformation(any)
      there was no(mockUploadIfRequiredAndNotExists).apply(any,any,any)
    }
  }


}
