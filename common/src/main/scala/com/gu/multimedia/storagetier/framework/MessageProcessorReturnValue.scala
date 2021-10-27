package com.gu.multimedia.storagetier.framework

import io.circe.Json

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/**
 * Represents a single "destination" for rabbitmq, i.e. the name of an exchange to publish to and a routing key
 * under which to send the message. Unlike the "normal" confirmation response, ".success" is not added to the routing key
 * here, it is used verbatim
 * @param outputExchange exchange name to send to
 * @param routingKey routing key to use for sending the message
 */
case class RMQDestination(outputExchange:String, routingKey:String)

/**
 * Represents a successful response from a MessageProcessor subclass.  Normally, you don't actually need to use this directly;
 * import the implicit converters with `import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._` and
 * you can just send a Json object.
 * If you need to split the message out to multiple destinations though, use:
 * ``
 * MessageProcessorReturnValue(someObject.asJson, Seq(RMQDestination("extra-exchange", "extra.routing.key"), ...))
 * ``
 * to tell the framework to push the message on to the extra destinations too
 * @param content Circe json object representing the message content to be output
 * @param additionalDestinations sequence (can be empty) of _extra_ destinations to send to, in addition to the confirmation
 *                               destination already set in the ProcessorConfiguration.
 */
case class MessageProcessorReturnValue(content:Json, additionalDestinations:Seq[RMQDestination])

object MessageProcessorReturnValue extends ((Json, Seq[RMQDestination])=>MessageProcessorReturnValue) {
  def apply(content:Json) = new MessageProcessorReturnValue(content, Seq())
}

/**
 * Implicit converters that allow you to pass a plain Circe Json object and get it automatically upcast
 * into a MessageProcessorReturnValue
 */
object MessageProcessorConverters {
  implicit def contentToMPRV(content:Json):MessageProcessorReturnValue = MessageProcessorReturnValue(content)
  implicit def futureEitherToMPRV(content:Future[Either[String, Json]])(implicit ec:ExecutionContext):Future[Either[String, MessageProcessorReturnValue]] = content.map({
    case Left(err)=>Left(err)
    case Right(content)=>Right(MessageProcessorReturnValue(content))
  })
}