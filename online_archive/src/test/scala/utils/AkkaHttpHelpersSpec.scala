package utils

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import plutocore.{AssetFolderLookup, AssetFolderRecord, PlutoCoreConfig}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import io.circe.generic.auto._
import java.nio.file.Paths
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class AkkaHttpHelpersSpec extends Specification with Mockito {
  "AkkaHttpHelpers.contentBodyToJson" should {
    "parse and extract the relevant data" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      system.dispatcher returns ExecutionContext.global

      val rawJsonString = """{"status":"ok","path":"/path/to/somefolder","project":"12345"}"""
      val result = Await.result(AkkaHttpHelpers.contentBodyToJson[AssetFolderRecord](Future(rawJsonString)), 1.second)
      result must beSome(AssetFolderRecord("/path/to/somefolder","12345"))
    }

    "fail the future if the data can't be parsed" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      system.dispatcher returns ExecutionContext.global


      val rawJsonString = """{"status":"ok","path":"/path/to/somefolder",project":"12345"}"""
      val result = Try { Await.result(AkkaHttpHelpers.contentBodyToJson[AssetFolderRecord](Future(rawJsonString)), 1.second) }
      result must beFailedTry
    }

    "fail the future if the data can't be unmarshalled" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      system.dispatcher returns ExecutionContext.global

      val rawJsonString = """{"status":"ok","path":"/path/to/somefolder"}"""
      val result = Try { Await.result(AkkaHttpHelpers.contentBodyToJson[AssetFolderRecord](Future(rawJsonString)), 1.second) }
      result must beFailedTry
    }
  }
}
