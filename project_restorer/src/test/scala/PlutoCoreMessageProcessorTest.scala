
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.{MXSConnectionBuilderImpl, MXSConnectionBuilderMock}
import com.gu.multimedia.mxscopy.models.{FileAttributes, MxsMetadata, ObjectMatrixEntry}
import com.om.mxs.client.japi.{MxsObject, Vault}

import io.circe.Json
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{OnlineOutputMessage, ProjectUpdateMessage}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Try

class PlutoCoreMessageProcessorTest(implicit ec: ExecutionContext) extends Specification with Mockito {
  val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", Some("internal-archive-vault"))

  implicit val mockActorSystem = mock[ActorSystem]
  implicit val mockMat = mock[Materializer]
  implicit val mockBuilder = mock[MXSConnectionBuilderImpl]


  val mockVault = mock[Vault]
  val mockObject = mock[MxsObject]
  mockVault.getObject(any) returns mockObject
  "OwnMessageProcessorTest" should {

    "drop message in handleMessage if wrong routing key" in {
      val toTest = new PlutoCoreMessageProcessor(mxsConfig)

      val emptyJson = Json.fromString("")

      val result = Try {
        Await.result(toTest.handleMessage("NOT.core.project.update", emptyJson), 3.seconds)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Not meant to receive this"
    }

    "return message with correct amount of associated files" in {
      implicit val mockActorSystem = mock[ActorSystem]
      val vault = mock[Vault]
      implicit val mockMat = mock[Materializer]
      implicit val mockBuilder = MXSConnectionBuilderMock(vault)



      val entry = MxsMetadata.empty
        .withValue("MXFS_PATH", "1234")
        .withValue("GNM_PROJECT_ID", 233)
        .withValue("GNM_TYPE", "rushes")

      val results = ObjectMatrixEntry("file1",Some(entry), None)


      val onlineOutput = OnlineOutputMessage.apply(results)
      val toTest = new PlutoCoreMessageProcessor(mxsConfig) {
        override def filesByProject(vault: Vault, projectId: String): Future[Seq[OnlineOutputMessage]] = Future(Seq(onlineOutput))
      }
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


      val expectedMessage = MessageSchema(
        1234,
        1
      )

      val result = Await.result(toTest.handleStatusMessage(updateMessage), 3.seconds)

      result must beRight(expectedMessage.asJson)
    }
  }
}
