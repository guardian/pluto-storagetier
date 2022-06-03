
import Main.mat
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{OnlineOutputMessage, ProjectUpdateMessage}
import org.junit.Assert.{assertThrows, fail}
import org.mockito.Mockito.when
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.util.Try

class PlutoCoreMessageProcessorTest extends Specification with Mockito {
  val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", Some("internal-archive-vault"))

  implicit val mockMat = mock[Materializer]
  implicit val mockBuilder = mock[MXSConnectionBuilderImpl]


  val mockVault = mock[Vault]
  val mockObject = mock[MxsObject]
  mockVault.getObject(any) returns mockObject
  "OwnMessageProcessorTest" should {
    "handleMessage" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig)

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
      val toTest = new PlutoCoreMessageProcessor(mxsConfig)

      val emptyJson = Json.fromString("")

      val result = Try {
        Await.result(toTest.handleMessage("NOT.core.project.update", emptyJson), 3.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Not meant to receive this"
    }

    "handleStatusMessage" in {

      val vault = mock[Vault]
      val onlineOutput = Seq(OnlineOutputMessage("NEARLINE",1234,"/path/to/file", "itemid", "nearlineId", "video"),
                            OnlineOutputMessage("NEARLINE",1234,"/path/to/file", "itemid", "nearlineId", "video"),
                            OnlineOutputMessage("NEARLINE",1234,"/path/to/file", "itemid", "nearlineId", "video"))

      val toTest = new PlutoCoreMessageProcessor(mxsConfig){

        override def filesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(onlineOutput)

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



      val result = Try{
        Await.result(toTest.handleStatusMessage(updateMessage), 3.seconds)
      }

      val failed_msg = "Failed to get status"


      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Failed to get status"

    }
  }
}
