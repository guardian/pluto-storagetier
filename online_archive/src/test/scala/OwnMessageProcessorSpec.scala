import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.{ArchiveHunterCommunicator, ArchiveHunterConfig}
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO, FailureRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{ShapeDocument, VidispineCommunicator}
import io.circe.Json
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._
import messages.RevalidateArchiveHunterRequest
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import java.time.ZonedDateTime
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Try
import AssetSweeperNewFile.Encoder._

class OwnMessageProcessorSpec extends Specification with Mockito {
  "OwnMessageProcessor.handleArchivehunterValidation" should {
    "perform validation through ArchiveHunterCommunicator and update the record" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future(true)
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor()

      val incomingRecord = ArchivedRecord(
        Some(1234),
        "abcdefg",
        false,
        "/from/root/path/to/some.file",
        100,
        "testbucket",
        "path/to/some.file",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      val result = Await.result(toTest.handleArchivehunterValidation(incomingRecord), 3.seconds)

      result must beRight(updatedRecord)
      there was one(archiveHunterCommunicator).lookupArchivehunterId("abcdefg","testbucket","path/to/some.file")
      there was one(archivedRecordDAO).updateIdValidationStatus(1234, true)
      there was one(archivedRecordDAO).getRecord(1234)
      there was no(failureRecordDAO).writeRecord(any)
    }

    "return a Left and not update the record if the ID is not valid" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future(false)
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor()

      val incomingRecord = ArchivedRecord(
        Some(1234),
        "abcdefg",
        false,
        "/from/root/path/to/some.file",
        100,
        "testbucket",
        "path/to/some.file",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      val result = Await.result(toTest.handleArchivehunterValidation(incomingRecord), 3.seconds)

      result must beLeft
      there was one(archiveHunterCommunicator).lookupArchivehunterId("abcdefg","testbucket","path/to/some.file")
      there was no(archivedRecordDAO).updateIdValidationStatus(any,any)
      there was no(archivedRecordDAO).getRecord(any)
      there was no(failureRecordDAO).writeRecord(any)
    }

    "pass on the failure if the lookup crashes for some reason" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future.failed(new RuntimeException("kaboom"))
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor()

      val incomingRecord = ArchivedRecord(
        Some(1234),
        "abcdefg",
        false,
        "/from/root/path/to/some.file",
        100,
        "testbucket",
        "path/to/some.file",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
      val result = Try { Await.result(toTest.handleArchivehunterValidation(incomingRecord), 3.seconds) }

      result must beFailedTry
      there was one(archiveHunterCommunicator).lookupArchivehunterId("abcdefg","testbucket","path/to/some.file")
      there was no(archivedRecordDAO).updateIdValidationStatus(any,any)
      there was no(archivedRecordDAO).getRecord(any)
      there was one(failureRecordDAO).writeRecord(any)
    }

  }

  "OwnMessageProcessor.handleRevalidationList" should {
    "look up each requested ID and then call handleArchivehunterValidation" in {
      val recordIds = Seq(3,5,7,11,13)
      val req = RevalidateArchiveHunterRequest(recordIds)

      val records = recordIds.map(id=>{
        val rec = ArchivedRecord(s"abcde$id",s"/path/to/file$id",1234L, "somebucket",s"file$id", None)
        rec.copy(id=Some(id))
      })

      val jsons = recordIds.map(id=>mock[Json])

      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.getRecord(any) returns Future(None)
      archivedRecordDAO.getRecord(3) returns Future(Some(records.head))
      archivedRecordDAO.getRecord(5) returns Future(Some(records(1)))
      archivedRecordDAO.getRecord(7) returns Future(Some(records(2)))
      archivedRecordDAO.getRecord(11) returns Future(Some(records(3)))
      archivedRecordDAO.getRecord(13) returns Future(Some(records(4)))

      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val mockHandleArchivehunterValidation = mock[(ArchivedRecord)=>Future[Either[String, ArchivedRecord]]]
      mockHandleArchivehunterValidation.apply(any[ArchivedRecord]) returns Future(Right(mock[ArchivedRecord]))
      val toTest = new OwnMessageProcessor() {
        override def handleArchivehunterValidation(rec:ArchivedRecord): Future[Either[String, ArchivedRecord]] = mockHandleArchivehunterValidation(rec)
      }

      val result = Try { Await.result(toTest.handleRevalidationList(req.asJson), 2.seconds) }
      result must beFailedTry(SilentDropMessage(Some("No return required")))
      there was one(archivedRecordDAO).getRecord(3)
      there was one(archivedRecordDAO).getRecord(5)
      there was one(archivedRecordDAO).getRecord(7)
      there was one(archivedRecordDAO).getRecord(11)
      there was one(archivedRecordDAO).getRecord(13)
      there was 5.times(archivedRecordDAO).getRecord(any)

      there was one(mockHandleArchivehunterValidation).apply(records.head)
      there was one(mockHandleArchivehunterValidation).apply(records(1))
      there was one(mockHandleArchivehunterValidation).apply(records(2))
      there was one(mockHandleArchivehunterValidation).apply(records(3))
      there was one(mockHandleArchivehunterValidation).apply(records(4))

      there was 5.times(mockHandleArchivehunterValidation).apply(any)
    }

    "skip records that are not found" in {
      val recordIds = Seq(3,5,7,11,13)
      val req = RevalidateArchiveHunterRequest(recordIds)

      val records = recordIds.map(id=>{
        val rec = ArchivedRecord(s"abcde$id",s"/path/to/file$id",1234L, "somebucket",s"file$id", None)
        rec.copy(id=Some(id))
      })

      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.getRecord(any) returns Future(None)
      archivedRecordDAO.getRecord(3) returns Future(Some(records.head))
      archivedRecordDAO.getRecord(5) returns Future(None)
      archivedRecordDAO.getRecord(7) returns Future(Some(records(2)))
      archivedRecordDAO.getRecord(11) returns Future(None)
      archivedRecordDAO.getRecord(13) returns Future(Some(records(4)))

      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val mockHandleArchivehunterValidation = mock[(ArchivedRecord)=>Future[Either[String, ArchivedRecord]]]
      mockHandleArchivehunterValidation.apply(any[ArchivedRecord]) returns Future(Right(mock[ArchivedRecord]))
      val toTest = new OwnMessageProcessor() {
        override def handleArchivehunterValidation(rec: ArchivedRecord): Future[Either[String, ArchivedRecord]] = mockHandleArchivehunterValidation(rec)
      }

      val result = Try { Await.result(toTest.handleRevalidationList(req.asJson), 2.seconds) }
      result must beFailedTry(SilentDropMessage(Some("No return required")))
      there was one(archivedRecordDAO).getRecord(3)
      there was one(archivedRecordDAO).getRecord(5)
      there was one(archivedRecordDAO).getRecord(7)
      there was one(archivedRecordDAO).getRecord(11)
      there was one(archivedRecordDAO).getRecord(13)
      there was 5.times(archivedRecordDAO).getRecord(any)

      there was one(mockHandleArchivehunterValidation).apply(records.head)
      there was one(mockHandleArchivehunterValidation).apply(records(2))
      there was one(mockHandleArchivehunterValidation).apply(records(4))

      there was 3.times(mockHandleArchivehunterValidation).apply(any)
    }
  }

  "OwnMessageProcessor.uploadVidispineBits" should {
    "list out vidispine shapes, request to upload each shape, thumbnails and metadata" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val shapes = Seq(
        ShapeDocument("VX-332","",Some(1),Seq("original"),Seq("video/mxf"),None,None,None),
        ShapeDocument("VX-334","",Some(1),Seq("lowres"),Seq("video/mp4"),None,None,None),
      )
      vidispineCommunicator.listItemShapes(any) returns Future(Some(shapes))

      vidispineFunctions.uploadThumbnailsIfRequired(any,any,any) returns Future(Right( () ))
      vidispineFunctions.uploadShapeIfRequired(any,any,org.mockito.ArgumentMatchers.eq("original"), any) returns Future.failed(SilentDropMessage())
      vidispineFunctions.uploadShapeIfRequired(any,any,org.mockito.ArgumentMatchers.eq("lowres"), any) answers((args:Array[AnyRef])=>Future(Right(MessageProcessorReturnValue(args(3).asInstanceOf[ArchivedRecord].asJson))))
      vidispineFunctions.uploadMetadataToS3(any,any,any) answers((args:Array[AnyRef])=>Future(Right(args(2).asInstanceOf[ArchivedRecord].asJson)))

      val rec = ArchivedRecord("abcdefg","/path/to/original.file", 1234L, "somebucket", "uploaded/file", Some(1)).copy(id=Some(1234),vidispineItemId = Some("VX-33"))
      val toTest = new OwnMessageProcessor()
      val result = Await.result(toTest.uploadVidispineBits("VX-33", rec), 2.seconds)

      result.head must beLeft("ignored")
      result(1) must beRight
      result.length mustEqual 2
      there was one(vidispineCommunicator).listItemShapes("VX-33")
      there was one(vidispineFunctions).uploadThumbnailsIfRequired("VX-33",None,rec)
      there were two(vidispineFunctions).uploadShapeIfRequired(any,any,any,any)
      there was one(vidispineFunctions).uploadShapeIfRequired("VX-33","VX-332","original",rec)
      there was one(vidispineFunctions).uploadShapeIfRequired("VX-33","VX-334", "lowres", rec)
      there was one(vidispineFunctions).uploadMetadataToS3("VX-33",None,rec)
    }

    "fail if thumbnail upload fails" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val shapes = Seq(
        ShapeDocument("VX-332","",Some(1),Seq("original"),Seq("video/mxf"),None,None,None),
        ShapeDocument("VX-334","",Some(1),Seq("lowres"),Seq("video/mp4"),None,None,None),
      )
      vidispineCommunicator.listItemShapes(any) returns Future(Some(shapes))

      vidispineFunctions.uploadThumbnailsIfRequired(any,any,any) returns Future.failed(new RuntimeException("Kaboom"))
      vidispineFunctions.uploadShapeIfRequired(any,any,org.mockito.ArgumentMatchers.eq("original"), any) returns Future.failed(SilentDropMessage())
      vidispineFunctions.uploadShapeIfRequired(any,any,org.mockito.ArgumentMatchers.eq("lowres"), any) answers((args:Array[AnyRef])=>Future(Right(MessageProcessorReturnValue(args(3).asInstanceOf[ArchivedRecord].asJson))))
      vidispineFunctions.uploadMetadataToS3(any,any,any) answers((args:Array[AnyRef])=>Future(Right(args(2).asInstanceOf[ArchivedRecord].asJson)))

      val rec = ArchivedRecord("abcdefg","/path/to/original.file", 1234L, "somebucket", "uploaded/file", Some(1)).copy(id=Some(1234),vidispineItemId = Some("VX-33"))
      val toTest = new OwnMessageProcessor()
      val result = Try { Await.result(toTest.uploadVidispineBits("VX-33", rec), 2.seconds) }
      result must beAFailedTry

      there was one(vidispineCommunicator).listItemShapes("VX-33")
      there was one(vidispineFunctions).uploadThumbnailsIfRequired("VX-33",None,rec)
    }
  }

  "OwnMessageProcessor.handleReplayStageTwo" should {
    "take an asset sweeper record, look up the corresponding ArchivedRecord (created by stage one) and call uploadVidispineBits then silently drop the message" in {
      val archivedRecord = ArchivedRecord("abcdefg","/path/to/original.file", 12345L, "some-bucket","uploaded/original.file", Some(1))
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(archivedRecord))
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val mockUploadVidispineBits = mock[(String, ArchivedRecord)=>Future[Seq[Either[String, MessageProcessorReturnValue]]]]
      mockUploadVidispineBits.apply(any,any) returns Future(Seq(Left("ignored"), Right(archivedRecord.asJson)))

      val mockHandleArchivehunterValidation = mock[(ArchivedRecord)=>Future[Either[String, ArchivedRecord]]]
      mockHandleArchivehunterValidation.apply(any) answers ((rec:Any)=>Future(Right(rec.asInstanceOf[ArchivedRecord])))

      val toTest = new OwnMessageProcessor() {
        override def handleArchivehunterValidation(rec: ArchivedRecord): Future[Either[String, ArchivedRecord]] = mockHandleArchivehunterValidation(rec)
        override def uploadVidispineBits(vidispineItemId: String, archivedRecord: ArchivedRecord): Future[Seq[Either[String, MessageProcessorReturnValue]]] = mockUploadVidispineBits(vidispineItemId, archivedRecord)
      }

      val msg = AssetSweeperNewFile(Some("VX-1234"),
        12345L, false, "video/mxf",
        ZonedDateTime.now(), ZonedDateTime.now(), ZonedDateTime.now(),
        123, 456, "/path/to", "original.file")

      val result = Try { Await.result(toTest.handleReplayStageTwo(msg.asJson), 2.seconds) }
      result must beASuccessfulTry
      result.get must beRight
      result.get.getOrElse(null).content.as[ArchivedRecord] must beRight(archivedRecord)

      there was one(mockHandleArchivehunterValidation).apply(archivedRecord)
      there was one(archivedRecordDAO).findBySourceFilename("/path/to/original.file")
      there was one(mockUploadVidispineBits).apply("VX-1234", archivedRecord)
    }

    "fail if there was no archived record found" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val mockUploadVidispineBits = mock[(String, ArchivedRecord)=>Future[Seq[Either[String, MessageProcessorReturnValue]]]]
      val toTest = new OwnMessageProcessor() {
        override def uploadVidispineBits(vidispineItemId: String, archivedRecord: ArchivedRecord): Future[Seq[Either[String, MessageProcessorReturnValue]]] = mockUploadVidispineBits(vidispineItemId, archivedRecord)
      }

      val msg = AssetSweeperNewFile(Some("VX-1234"),
        12345L, false, "video/mxf",
        ZonedDateTime.now(), ZonedDateTime.now(), ZonedDateTime.now(),
        123, 456, "/path/to", "original.file")

      val result = Try { Await.result(toTest.handleReplayStageTwo(msg.asJson), 2.seconds) }
      result must beFailedTry
      result.failed.get.getMessage mustEqual("The given asset sweeper record for /path/to/original.file is not imported yet")

      there was one(archivedRecordDAO).findBySourceFilename("/path/to/original.file")
      there was no(mockUploadVidispineBits).apply(any, any)
    }

    "not hide an error in the upload function" in {
      val archivedRecord = ArchivedRecord("abcdefg","/path/to/original.file", 12345L, "some-bucket","uploaded/original.file", Some(1))
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(archivedRecord))
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val mockUploadVidispineBits = mock[(String, ArchivedRecord)=>Future[Seq[Either[String, MessageProcessorReturnValue]]]]
      mockUploadVidispineBits.apply(any,any) returns Future.failed(new RuntimeException("Kaboom!"))

      val mockHandleArchivehunterValidation = mock[(ArchivedRecord)=>Future[Either[String, ArchivedRecord]]]
      mockHandleArchivehunterValidation.apply(any) answers ((rec:Any)=>Future(Right(rec.asInstanceOf[ArchivedRecord])))

      val toTest = new OwnMessageProcessor() {
        override def handleArchivehunterValidation(rec: ArchivedRecord): Future[Either[String, ArchivedRecord]] = mockHandleArchivehunterValidation(rec)
        override def uploadVidispineBits(vidispineItemId: String, archivedRecord: ArchivedRecord): Future[Seq[Either[String, MessageProcessorReturnValue]]] = mockUploadVidispineBits(vidispineItemId, archivedRecord)
      }

      val msg = AssetSweeperNewFile(Some("VX-1234"),
        12345L, false, "video/mxf",
        ZonedDateTime.now(), ZonedDateTime.now(), ZonedDateTime.now(),
        123, 456, "/path/to", "original.file")

      val result = Try { Await.result(toTest.handleReplayStageTwo(msg.asJson), 2.seconds) }
      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Kaboom!"

      there was one(archivedRecordDAO).findBySourceFilename("/path/to/original.file")
      there was one(mockUploadVidispineBits).apply("VX-1234", archivedRecord)
    }

    "return a retryable failure if the validation fails" in {
      val archivedRecord = ArchivedRecord("abcdefg","/path/to/original.file", 12345L, "some-bucket","uploaded/original.file", Some(1))
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      archivedRecordDAO.findBySourceFilename(any) returns Future(Some(archivedRecord))
      implicit val failureRecordDAO = mock[FailureRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]
      implicit val vidispineFunctions = mock[VidispineFunctions]

      val mockUploadVidispineBits = mock[(String, ArchivedRecord)=>Future[Seq[Either[String, MessageProcessorReturnValue]]]]
      mockUploadVidispineBits.apply(any,any) returns Future.failed(new RuntimeException("Kaboom!"))

      val mockHandleArchivehunterValidation = mock[(ArchivedRecord)=>Future[Either[String, ArchivedRecord]]]
      mockHandleArchivehunterValidation.apply(any) answers ((rec:Any)=>Future(Left("ArchiveHunter ID is not validated yet")))

      val toTest = new OwnMessageProcessor() {
        override def handleArchivehunterValidation(rec: ArchivedRecord): Future[Either[String, ArchivedRecord]] = mockHandleArchivehunterValidation(rec)
        override def uploadVidispineBits(vidispineItemId: String, archivedRecord: ArchivedRecord): Future[Seq[Either[String, MessageProcessorReturnValue]]] = mockUploadVidispineBits(vidispineItemId, archivedRecord)
      }

      val msg = AssetSweeperNewFile(Some("VX-1234"),
        12345L, false, "video/mxf",
        ZonedDateTime.now(), ZonedDateTime.now(), ZonedDateTime.now(),
        123, 456, "/path/to", "original.file")

      val result = Try { Await.result(toTest.handleReplayStageTwo(msg.asJson), 2.seconds) }
      result must beASuccessfulTry
      result.get must beLeft("ArchiveHunter ID is not validated yet")

      there was one(archivedRecordDAO).findBySourceFilename("/path/to/original.file")
      there was one(mockHandleArchivehunterValidation).apply(archivedRecord)
      there was no(mockUploadVidispineBits).apply("VX-1234", archivedRecord)
    }
  }
}
