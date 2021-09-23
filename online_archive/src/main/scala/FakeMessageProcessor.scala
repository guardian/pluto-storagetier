import com.gu.multimedia.storagetier.framework.MessageProcessor
import io.circe.Json
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class FakeMessageProcessor (db:Database) extends MessageProcessor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def handleMessage(routingKey:String, msg: Json): Future[Either[String, Json]] = {
    logger.info(s"Received message of $routingKey from queue: ${msg.noSpaces}")
    Future(Left("Received ok, testing"))
  }
}
