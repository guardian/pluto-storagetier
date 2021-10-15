package com.gu.multimedia.storagetier.vidispine

import akka.actor.ActorSystem
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.headers.{Accept, Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, HttpResponse, MediaRange, MediaTypes, StatusCodes}
import akka.stream.Materializer
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

class VidispineCommunicatorSpec extends Specification with AfterAll with Mockito {
  implicit val actorSystem:ActorSystem = ActorSystem("VidispineCommunicatorSpec")
  implicit val mat:Materializer = Materializer.matFromSystem

  override def afterAll() = {
    Await.ready(actorSystem.terminate(), 30.seconds)
  }

  def readSampleDoc(resource: String) = {
    val s = Source.fromResource(resource)
    val content = s.mkString
    s.close()
    content
  }

  val fakeConfig = VidispineConfig("https://test-case","test","test")
  "VidispineCommunicator.findItemShape" should {
    "make a request to /API/item/shape for a video and unmarshal the return value" in {
      val rawJsonShapeDoc = readSampleDoc("sample_shape_doc.json")
      val response = HttpResponse(StatusCodes.OK, entity = HttpEntity(rawJsonShapeDoc))
      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(response)
      val toTest = new VidispineCommunicator(fakeConfig) {
        override def callHttp: HttpExt = mockHttp
      }

      val result = Await.result(toTest.findItemShape("VX-123","VX-456"), 1.second)

      there was one(mockHttp).singleRequest(org.mockito.ArgumentMatchers.eq(
        HttpRequest(
          HttpMethods.GET,
          "https://test-case/API/item/VX-123/shape/VX-456",
          List(
            Accept(MediaRange(MediaTypes.`application/json`)),
            Authorization(BasicHttpCredentials("test","test"))
          )
        )
      ),any,any,any)
      result must beSome
      result.get.id mustEqual "VX-151335"
      result.get.mimeType mustEqual Seq("video/mp4")
      result.get.tag mustEqual Seq("lowres")
      result.get.getLikelyFile must beSome(
        VSShapeFile(
          "VX-735825",
          "VX-735825.mp4",
          Seq("file:///path/to/vidispine/Proxies/VX-735825.mp4"),
          "CLOSED",
          83401958L,
          Some("9a4192d38e12997716d9b389d73e6170614c7186"),
          "2021-10-05T13:18:08.374+0000",
          1,
          "VX-6"
        )
      )
    }

    "make a request to /API/item/shape for a video with no audio and unmarshal the return value" in {
      val rawJsonShapeDoc = readSampleDoc("sample_shape_doc_video_no_audio.json")
      val response = HttpResponse(StatusCodes.OK, entity = HttpEntity(rawJsonShapeDoc))
      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(response)
      val toTest = new VidispineCommunicator(fakeConfig) {
        override def callHttp: HttpExt = mockHttp
      }

      val result = Await.result(toTest.findItemShape("VX-123","VX-456"), 1.second)

      there was one(mockHttp).singleRequest(org.mockito.ArgumentMatchers.eq(
        HttpRequest(
          HttpMethods.GET,
          "https://test-case/API/item/VX-123/shape/VX-456",
          List(
            Accept(MediaRange(MediaTypes.`application/json`)),
            Authorization(BasicHttpCredentials("test","test"))
          )
        )
      ),any,any,any)
      result must beSome
      result.get.id mustEqual "VX-151335"
      result.get.mimeType mustEqual Seq("video/mp4")
      result.get.tag mustEqual Seq("mp4-noaudio")
      result.get.getLikelyFile must beSome(
        VSShapeFile(
          "VX-735825",
          "VX-735825.mp4",
          Seq("file:///path/to/vidispine/Proxies/VX-735825.mp4"),
          "CLOSED",
          189325589L,
          Some("9d304c3e23bdbe1f153c746679fd2f1f6846f561"),
          "2018-09-19T14:06:44.435+0000",
          1,
          "VX-6"
        )
      )
    }

    "make a request to /API/item/shape for an audio only and unmarshal the return value" in {
      val rawJsonShapeDoc = readSampleDoc("sample_shape_audio_only_doc.json")
      val response = HttpResponse(StatusCodes.OK, entity = HttpEntity(rawJsonShapeDoc))
      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(response)
      val toTest = new VidispineCommunicator(fakeConfig) {
        override def callHttp: HttpExt = mockHttp
      }

      val result = Await.result(toTest.findItemShape("VX-123","VX-456"), 1.second)

      there was one(mockHttp).singleRequest(org.mockito.ArgumentMatchers.eq(
        HttpRequest(
          HttpMethods.GET,
          "https://test-case/API/item/VX-123/shape/VX-456",
          List(
            Accept(MediaRange(MediaTypes.`application/json`)),
            Authorization(BasicHttpCredentials("test","test"))
          )
        )
      ),any,any,any)
      result must beSome
      result.get.id mustEqual "VX-5626"
      result.get.mimeType mustEqual Seq("audio/x-wav")
      result.get.tag mustEqual Seq("wav-8track")
      result.get.getLikelyFile must beSome(
        VSShapeFile(
          "VX-4222",
          "VX-4222.wav",
          Seq("file://path/to/vidispine/Audio/VX-4222.wav"),
          "CLOSED",
          231546930L,
          Some("08a55101bb7f0afd9ca919817b5fbb59d86bba5e"),
          "2020-01-17T15:50:05.210+0000",
          1,
          "VX-6"
        )
      )
    }

    "return None if /API/item/shape returns 404" in {
      val response = HttpResponse(StatusCodes.NotFound)
      val mockHttp = mock[HttpExt]
      mockHttp.singleRequest(any,any,any,any) returns Future(response)
      val toTest = new VidispineCommunicator(fakeConfig) {
        override def callHttp: HttpExt = mockHttp
      }

      val result = Await.result(toTest.findItemShape("VX-123","VX-456"), 1.second)

      there was one(mockHttp).singleRequest(org.mockito.ArgumentMatchers.eq(
        HttpRequest(
          HttpMethods.GET,
          "https://test-case/API/item/VX-123/shape/VX-456",
          List(
            Accept(MediaRange(MediaTypes.`application/json`)),
            Authorization(BasicHttpCredentials("test","test"))
          )
        )
      ),any,any,any)
      result must beNone
    }


  }
}
