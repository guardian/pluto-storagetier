import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessor, MessageProcessorReturnValue}
import io.circe.Json
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.{ExecutionContext, Future}

class FakeMessageProcessor (db:Database)(implicit ec:ExecutionContext) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def handleMessage(routingKey:String, msg: Json, framework: MessageProcessingFramework): Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"Received message of $routingKey from queue: ${msg.noSpaces}")
    Future(Left("Received ok, testing"))
  }
}
