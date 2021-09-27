package utils

import akka.http.scaladsl.model.ResponseEntity
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * this object can be imported into communicator classes, and supplies routines to buffer the response entity into
 * memory and to parse/unmarshal it as JSON
 */
object AkkaHttpHelpers {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * internal method that consumes a given response entity to a String
   *
   * @param entity ResponseEntity object
   * @return a Future containing the String of the content
   */
  def consumeResponseEntity(entity: ResponseEntity)(implicit mat:Materializer, ec:ExecutionContext) = {
    val sink = Sink.reduce[ByteString]((acc, elem) => acc ++ elem)
    entity.dataBytes.toMat(sink)(Keep.right).run().map(_.utf8String)
  }

  /**
   * convenience method that consumes a given response entity and parses it into a Json object
   *
   * @param entity ResponseEntity object
   * @return a Future containing either the ParsingFailure error or the parsed Json object
   */
  def consumeResponseEntityJson(entity: ResponseEntity)(implicit mat:Materializer, ec:ExecutionContext) = consumeResponseEntity(entity)
    .map(io.circe.parser.parse)

  def contentBodyToJson[T: io.circe.Decoder](contentBody: Future[String])(implicit mat:Materializer, ec:ExecutionContext) = contentBody
    .map(io.circe.parser.parse)
    .map(_.map(json => (json \\ "result").headOption.getOrElse(json))) //if the actual object is hidden under a "result" field take that
    .map(_.flatMap(_.as[T]))
    .map({
      case Left(err) =>
        logger.error(s"Problematic response: ${contentBody.value}")
        throw new RuntimeException("Could not understand server response: ", err)
      case Right(data) => Some(data)
    })
}
