package archivehunter

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse, ResponseEntity, StatusCodes, Uri}
import akka.stream.Materializer
import akka.util.ByteString
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import io.circe.syntax._
import io.circe.generic.auto._
import org.mockito.ArgumentMatcher
import utils.ArchiveHunter

import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class ArchiveHunterCommunicatorSpec extends Specification with AfterAll with Mockito {
  implicit val actorSystem = ActorSystem("ArchiveHunterCommunicatorSpec")
  implicit val mat:Materializer = Materializer.matFromSystem
  implicit val ec:ExecutionContext = actorSystem.dispatcher

  override def afterAll(): Unit = Await.ready(actorSystem.terminate(), 10.seconds)

  "ArchiveHunterCommunicator.getToken" should {
    "generate a base64 encoded signature unique for the given time" in {
      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://fake-uri","nosecret")) {
        def callGetToken(uri:Uri,formattedTime:String, contentLength:Int, requestMethod:String, contentChecksum:String) = getToken(uri, formattedTime, contentLength, requestMethod, contentChecksum)
      }

      val t = ZonedDateTime.of(2012,1,2,3,4,5,0,ZoneId.of("UTC"))
      val formattedTime = toTest.httpDateFormatter.format(t)

      toTest.callGetToken("https://some-server/some-endpoint",formattedTime, 1234,"POST", "abcdefg") mustEqual "9dCGC8Vr2FPQ2ZaHhdbnaoKC5NLdHIhT7fYqJ9qk7uBP5vKHB77F2TQUxM8r2jYt"

      val anotherFormattedTime = toTest.httpDateFormatter.format(ZonedDateTime.of(2012,1,2,3,4,12,0,ZoneId.of("UTC")))
      toTest.callGetToken("https://some-server/some-endpoint",anotherFormattedTime, 1234,"POST", "abcdefg") mustNotEqual "9dCGC8Vr2FPQ2ZaHhdbnaoKC5NLdHIhT7fYqJ9qk7uBP5vKHB77F2TQUxM8r2jYt"
    }
  }

  "ArchiveHunterCommunicator.lookupArchiveHunterId" should {
    "return False if the server returns 404" in {
      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(HttpResponse(StatusCodes.NotFound))

      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://fake-uri","nosecret")) {
        override def callHttp = mockHttp
      }

      val result = Await.result(toTest.lookupArchivehunterId("abcdefg","somebucket","path/to/some.file"), 5.seconds)
      result must beFalse
    }

    "return True if the server returns 200 with matching content" in {
      val response = ArchiveHunterResponses.ArchiveHunterGetResponse(
        "ok",
        "entry",
        ArchiveHunterResponses.ArchiveEntry(
          "abcdefg",
          "somebucket",
          "path/to/some.file",
          Some("us-east-1"),
          None,
          1234567L,
          ZonedDateTime.now(),
          "some-etag",
          ArchiveHunterResponses.MimeType("video","mp4"),
          true,
          "STANDARD-IA",
          false
        )
      ).asJson.noSpaces

      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(
        HttpResponse(
          StatusCodes.OK,
          entity=HttpEntity.Strict(ContentTypes.`application/json`,ByteString(response))
        )
      )

      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://fake-uri","nosecret")) {
        override def callHttp = mockHttp
      }

      val result = Await.result(toTest.lookupArchivehunterId("abcdefg","somebucket","path/to/some.file"), 5.seconds)
      result must beTrue
    }

    "return a failure if the server returns 200 with non-matching content" in {
      val response = ArchiveHunterResponses.ArchiveHunterGetResponse(
        "ok",
        "entry",
        ArchiveHunterResponses.ArchiveEntry(
          "abcdefg",
          "somebucket",
          "path/to/some.file",
          Some("us-east-1"),
          None,
          1234567L,
          ZonedDateTime.now(),
          "some-etag",
          ArchiveHunterResponses.MimeType("video","mp4"),
          true,
          "STANDARD-IA",
          false
        )
      ).asJson.noSpaces

      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(
        HttpResponse(
          StatusCodes.OK,
          entity=HttpEntity.Strict(ContentTypes.`application/json`,ByteString(response))
        )
      )

      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://fake-uri","nosecret")) {
        override def callHttp = mockHttp
      }

      val result = Try { Await.result(toTest.lookupArchivehunterId("abcdefg","somebucket","path/to/another.file"), 5.seconds) }
      result must beAFailedTry
      result.failed.get.getMessage must contain("The ID links to another file")
    }

    "return a failure if the server returns 200 with an incorrect object type" in {
      val response = ArchiveHunterResponses.ArchiveHunterGetResponse(
        "ok",
        "somethingelse",
        ArchiveHunterResponses.ArchiveEntry(
          "abcdefg",
          "somebucket",
          "path/to/some.file",
          Some("us-east-1"),
          None,
          1234567L,
          ZonedDateTime.now(),
          "some-etag",
          ArchiveHunterResponses.MimeType("video","mp4"),
          true,
          "STANDARD-IA",
          false
        )
      ).asJson.noSpaces

      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(
        HttpResponse(
          StatusCodes.OK,
          entity=HttpEntity.Strict(ContentTypes.`application/json`,ByteString(response))
        )
      )

      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://fake-uri","nosecret")) {
        override def callHttp = mockHttp
      }

      val result = Try { Await.result(toTest.lookupArchivehunterId("abcdefg","somebucket","path/to/another.file"), 5.seconds) }
      result must beAFailedTry
      result.failed.get.getMessage must contain("Expected an entity type of 'entry'")
    }

    "return a failure if the server returns permission denied" in {
      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(
        HttpResponse(
          StatusCodes.Forbidden,
        )
      )

      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://fake-uri","nosecret")) {
        override def callHttp = mockHttp
      }

      val result = Try { Await.result(toTest.lookupArchivehunterId("abcdefg","somebucket","path/to/another.file"), 5.seconds) }
      result must beAFailedTry
      result.failed.get.getMessage must contain("Archive Hunter said permission denied")
    }
  }

  "ArchiveHunterCommunicator.importProxy" should {
    "call out to importProxy via POST" in {
      val mockHttp = mock[HttpExt]
      val responseRawJson = """{"status":"success"}"""
      mockHttp.singleRequest(any,any,any,any) returns Future(HttpResponse(StatusCodes.OK, entity=HttpEntity(responseRawJson)))
      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://test-url","notasecret")) {
        override def callHttp: HttpExt = mockHttp
      }

      val result = Try { Await.result(toTest.importProxy("abcdefg", "path/to/some/file.upload", "proxy-bucket", ArchiveHunter.ProxyType.VIDEO), 10.seconds) }
      result must beASuccessfulTry

      case class ExpectedRequest() extends ArgumentMatcher[HttpRequest] {
        override def matches(argument: HttpRequest): Boolean = {
          import com.gu.multimedia.storagetier.utils.AkkaHttpHelpers.consumeResponseEntity
          val expectedRequestContent = """{"itemId":"abcdefg","proxyPath":"path/to/some/file.upload","proxyBucket":"proxy-bucket","proxyType":"VIDEO"}"""
          val entityContent = Await.result(consumeResponseEntity(argument.entity), 2.seconds)
          argument.uri==Uri("https://test-url/api/importProxy") && argument.method==HttpMethods.POST && entityContent==expectedRequestContent
        }
      }

      there was one(mockHttp).singleRequest(
        org.mockito.ArgumentMatchers.argThat(ExpectedRequest()),
        any,
        any,
        any
      )
    }

    "fail the future if a NotFound is returned" in {
      val mockHttp = mock[HttpExt]
      val responseRawJson = """{"status":"not_found"}"""
      mockHttp.singleRequest(any,any,any,any) returns Future(HttpResponse(StatusCodes.NotFound, entity=HttpEntity(responseRawJson)))
      val toTest = new ArchiveHunterCommunicator(ArchiveHunterConfig("https://test-url","notasecret")) {
        override def callHttp: HttpExt = mockHttp
      }

      val result = Try { Await.result(toTest.importProxy("abcdefg", "path/to/some/file.upload", "proxy-bucket", ArchiveHunter.ProxyType.VIDEO), 10.seconds) }
      result must beAFailedTry
      result.failed.get.getMessage mustEqual "The item ID was not found"

      there was one(mockHttp).singleRequest(
        any,
        any,
        any,
        any
      )
    }
  }
}
