import Main.{actorSystem, mat}
import akka.dispatch.japi
import akka.http.impl.engine.parsing.ParserOutput
import akka.http.scaladsl.client
import akka.http.scaladsl.client.TransformerPipelineSupport
import akka.http.scaladsl.server.util.ConstructFromTuple
import akka.http.scaladsl.server.{PathMatcher, RejectionHandler}
import akka.stream.Materializer
import akka.stream.impl.fusing.MapAsync
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.data.AndThen
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue}
import com.om.mxs.client.japi.Vault
import io.circe.Json
import io.circe.generic.auto.{exportDecoder, exportEncoder}
import io.circe.syntax.EncoderOps
import matrixstore.MatrixStoreConfig
import messages.{OnlineOutputMessage, ProjectUpdateMessage}
import org.slf4j.LoggerFactory
import shapeless.ops.record.Extractor
import shapeless.ops.{coproduct, hlist, tuple}
import slick.compiler.{InsertCompiler, Phase}
import slick.jdbc.{ConnectionPreparer, GetResult}
import slick.lifted.{CanBeQueryCondition, OptionMapper}
import slick.memory.QueryInterpreter

import scala.Console.err
import scala.collection.SetOps
import scala.compat.java8.{JFunction1, JProcedure1}
import scala.compat.java8.functionConverterImpls.{FromJavaConsumer, FromJavaDoubleConsumer, FromJavaDoubleFunction, FromJavaDoublePredicate, FromJavaDoubleToIntFunction, FromJavaDoubleToLongFunction, FromJavaDoubleUnaryOperator, FromJavaFunction, FromJavaIntConsumer, FromJavaIntFunction, FromJavaIntPredicate, FromJavaIntToDoubleFunction, FromJavaIntToLongFunction, FromJavaIntUnaryOperator, FromJavaLongConsumer, FromJavaLongFunction, FromJavaLongPredicate, FromJavaLongToDoubleFunction, FromJavaLongToIntFunction, FromJavaLongUnaryOperator, FromJavaPredicate, FromJavaToDoubleFunction, FromJavaToIntFunction, FromJavaToLongFunction, FromJavaUnaryOperator}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.impl.{FutureConvertersImpl, Promise}
import scala.concurrent.java8.FuturesConvertersImpl
import scala.jdk.FunctionWrappers
import scala.runtime.{AbstractFunction1, AbstractPartialFunction}
import scala.xml.dtd.ElementValidator
import scala.xml.persistent.Index
import scala.xml.transform.BasicTransformer


class PlutoCoreMessageProcessor(mxsConfig:MatrixStoreConfig)(implicit mat:Materializer,mxsConnectionBuilder:MXSConnectionBuilderImpl)
  extends
  MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)


  def filesByProject(vault:Vault, projectId:String) = {
    val sinkFactory = Sink.seq[OnlineOutputMessage]
    Source.fromGraph(new OMFastContentSearchSource(vault,
      s"GNM_PROJECT_ID:\"$projectId\"",
      Array("MXFS_PATH","MXFS_FILENAME", "__mxs__length")
    )
    ).map(OnlineOutputMessage.apply)
      .toMat(sinkFactory)(Keep.right)
      .run()
  }


  def searchAssociatedMedia(projectId: Int, vault: Vault)  = {
   for {
     result <- filesByProject(vault, projectId.toString)
   } yield result
  }

  def handleStatusMessage(updateMessage: ProjectUpdateMessage) = {
    mxsConnectionBuilder.withVaultFuture(mxsConfig.nearlineVaultId) {vault =>

        searchAssociatedMedia(updateMessage.id, vault)
      }

    )

  }

  /**
   * Override this method in your subclass to handle an incoming message
   *
   * @param routingKey the routing key of the message as received from the broker.
   * @param msg        the message body, as a circe Json object. You can unmarshal this into a case class by
   *                   using msg.as[CaseClassFormat]
   * @return You need to return Left() with a descriptive error string if the message could not be processed, or Right
   *         with a circe Json body (can be done with caseClassInstance.noSpaces) containing a message body to send
   *         to our exchange with details of the completed operation
   */
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    routingKey match {
      case "core.project.update" =>
        logger.info(s"Received message of $routingKey from queue: ${msg.noSpaces}")
        msg.as[ProjectUpdateMessage] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not unmarshal json message ${msg.noSpaces} into an ProjectUpdate: $err"))
          case Right(updateMessage) =>
            logger.info(s"here is an update status ${updateMessage.status}")
            handleStatusMessage(updateMessage)
        }
      case _ =>
        logger.warn(s"Dropping message $routingKey from own exchange as I don't know how to handle it. This should be fixed in the code.")
        Future.failed(new RuntimeException("Not meant to receive this"))
    }
  }
}
