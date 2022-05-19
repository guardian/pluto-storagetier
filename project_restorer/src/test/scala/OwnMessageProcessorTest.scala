
import io.circe.Json
import io.circe.syntax.EncoderOps
import messages.ProjectUpdateMessage
import org.junit.Assert.{assertThrows, fail}
import org.mockito.Mockito.when
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Try

class OwnMessageProcessorTest extends Specification with Mockito {

  "OwnMessageProcessorTest" should {
    "handleMessage" in {
      val toTest = new OwnMessageProcessor()

      val msgContent =
        """  {
          |    "id": 23460,
          |    "projectTypeId": 45,
          |    "title": "anothertest",
          |    "created": "2021-03-05T16:00:46Z",
          |    "updated": "2021-03-05T16:00:46Z",
          |    "user": "andy_gallagher",
          |    "workingGroupId": 17,
          |    "commissionId": 3555,
          |    "deletable": true,
          |    "deep_archive": false,
          |    "sensitive": true,
          |    "status": "Completed",
          |    "productionOffice": "UK"
          |  }
          |""".stripMargin

      val msg = io.circe.parser.parse(msgContent)
      val emptyJson = Json.fromString("")

      val result = Try{ Await.result(toTest.handleMessage("core.project.update", msg.getOrElse(emptyJson)), 3.seconds)}
      
      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Failed to get status"
    }

    "drop message in handleMessage if wrong routing key" in {
      val toTest = new OwnMessageProcessor()

      val emptyJson = Json.fromString("")

      val result = Try {
        Await.result(toTest.handleMessage("NOT.core.project.update", emptyJson), 3.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Not meant to receive this"
    }

    "handleStatusMessage" in {
      val toTest = new OwnMessageProcessor()

      val updateMessage = ProjectUpdateMessage(
        1234,
        2,
        "abcdefg",
        Option.empty,
        Option.empty,
        "le user",
        100,
        200,
        deletable = true,
        deep_archive = false,
        sensitive = false,
        "oh noooo",
        "LDN"
      )
      val result = Try{Await.result(toTest.handleStatusMessage(updateMessage), 3.seconds)}

      val failed_msg = "Failed to get status"


      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Failed to get status"

    }
  }
}
