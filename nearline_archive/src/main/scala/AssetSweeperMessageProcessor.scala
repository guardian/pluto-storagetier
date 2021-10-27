import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, SilentDropMessage}
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecordDAO}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class AssetSweeperMessageProcessor()
                                  (implicit nearlineRecordDAO: NearlineRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   ec:ExecutionContext,
                                   mat:Materializer,
                                   system:ActorSystem,
                                   matrixStoreBuilder: MXSConnectionBuilder) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = {
    if(!routingKey.endsWith("new") && !routingKey.endsWith("update")) return Future.failed(SilentDropMessage())
    msg.as[AssetSweeperNewFile] match {
      case Left(err)=>
        Future(Left(s"Could not parse incoming message: $err"))
      case Right(_)=>
        logger.warn("Received an message, these are not implemented yet")
        Future(Left("Not implemented yet"))
    }
  }
}
