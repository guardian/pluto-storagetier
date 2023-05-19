import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, MessageProcessorReturnValue, RMQDestination}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import io.circe.Json
import matrixstore.MatrixStoreConfig

import org.slf4j.MDC

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._


import scala.concurrent.{ExecutionContext, Future}

class MediaRemoverMessageProcessor(implicit nearlineRecordDAO: NearlineRecordDAO,
                                   failureRecordDAO: FailureRecordDAO,
                                   vidispineCommunicator: VidispineCommunicator,
                                   ec: ExecutionContext,
                                   mat: Materializer,
                                   system: ActorSystem,
                                   matrixStoreBuilder: MXSConnectionBuilder,
                                   mxsConfig: MatrixStoreConfig,
                                   fileCopier: FileCopier) extends MessageProcessor {
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
  override def handleMessage(routingKey: String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] =
    routingKey match {
      case r if r == "storagetier.nearline.internalarchive.required.nearline" || r == "storagetier.nearline.internalarchive.required.online" =>
        msg.as[NearlineRecord] match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Could not parse message as a nearline record: $err"))
          case Right(rec) =>
            MDC.put("correlationId", rec.correlationId)
            Future(Right(MessageProcessorReturnValue(rec.asJson))) // Just forward it, will be on ownExchangeName
          case _ =>
            Future(Left(s"Unrecognised routing key: $routingKey"))
        }
    }

}
