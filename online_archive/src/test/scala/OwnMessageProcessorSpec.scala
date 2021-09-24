import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.{ArchiveHunterCommunicator, ArchiveHunterConfig}
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecord, ArchivedRecordDAO}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Try

class OwnMessageProcessorSpec extends Specification with Mockito {
  "OwnMessageProcessor.handleArchivehunterValidation" should {
    "perform validation through ArchiveHunterCommunicator and update the record" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future(true)
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor(ArchiveHunterConfig("https://some.base","secret-goes-here"))

      val incomingRecord = ArchivedRecord(
        Some(1234),
        "abcdefg",
        false,
        "/from/root/path/to/some.file",
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
      val result = Await.result(toTest.handleArchivehunterValidation(incomingRecord.asJson), 3.seconds)

      result must beRight(updatedRecord)
      there was one(archiveHunterCommunicator).lookupArchivehunterId("abcdefg","testbucket","path/to/some.file")
      there was one(archivedRecordDAO).updateIdValidationStatus(1234, true)
      there was one(archivedRecordDAO).getRecord(1234)
    }

    "return a Left and not update the record if the ID is not valid" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future(false)
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor(ArchiveHunterConfig("https://some.base","secret-goes-here"))

      val incomingRecord = ArchivedRecord(
        Some(1234),
        "abcdefg",
        false,
        "/from/root/path/to/some.file",
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
      val result = Await.result(toTest.handleArchivehunterValidation(incomingRecord.asJson), 3.seconds)

      result must beLeft
      there was one(archiveHunterCommunicator).lookupArchivehunterId("abcdefg","testbucket","path/to/some.file")
      there was no(archivedRecordDAO).updateIdValidationStatus(any,any)
      there was no(archivedRecordDAO).getRecord(any)
    }

    "pass on the failure if the lookup crashes for some reason" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future.failed(new RuntimeException("kaboom"))
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor(ArchiveHunterConfig("https://some.base","secret-goes-here"))

      val incomingRecord = ArchivedRecord(
        Some(1234),
        "abcdefg",
        false,
        "/from/root/path/to/some.file",
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
      val result = Try { Await.result(toTest.handleArchivehunterValidation(incomingRecord.asJson), 3.seconds) }

      result must beFailedTry
      there was one(archiveHunterCommunicator).lookupArchivehunterId("abcdefg","testbucket","path/to/some.file")
      there was no(archivedRecordDAO).updateIdValidationStatus(any,any)
      there was no(archivedRecordDAO).getRecord(any)
    }

    "return a failure if the given message is not in the right format" in {
      implicit val mat:Materializer = mock[Materializer]
      implicit val system:ActorSystem = mock[ActorSystem]
      implicit val archivedRecordDAO = mock[ArchivedRecordDAO]
      implicit val archiveHunterCommunicator = mock[ArchiveHunterCommunicator]

      archiveHunterCommunicator.lookupArchivehunterId(any,any,any) returns Future.failed(new RuntimeException("kaboom"))
      val updatedRecord = mock[ArchivedRecord]
      archivedRecordDAO.getRecord(any) returns Future(Some(updatedRecord))
      archivedRecordDAO.updateIdValidationStatus(any,any) returns Future(1)

      val toTest = new OwnMessageProcessor(ArchiveHunterConfig("https://some.base","secret-goes-here"))

      val incomingRecord = Map[String,String]("key"->"value")
      val result = Try { Await.result(toTest.handleArchivehunterValidation(incomingRecord.asJson), 3.seconds) }

      result must beFailedTry
      there was no(archiveHunterCommunicator).lookupArchivehunterId(any,any,any)
      there was no(archivedRecordDAO).updateIdValidationStatus(any,any)
      there was no(archivedRecordDAO).getRecord(any)
    }
  }
}
