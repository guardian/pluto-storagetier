package com.gu.multimedia.storagetier.framework

import java.util.UUID

/**
 * Represents an individual configuration for MessageProcessor
 *
 * @param exchangeName The exchange name to bind to
 * @param routingKey   The routing key spec to listen to. Wildcards (#, *) are allowed. For all messages specify '#'.
 * @param outputRoutingKey The routing key used for output. Wildcards not allowed. ".success" is appended on success
 * @param processor instance of a MessageProcessor class to handle the messages
 */
case class ProcessorConfiguration(exchangeName:String,
                                  routingKey:Seq[String],
                                  outputRoutingKey:String,
                                  processor:MessageProcessor,
                                  testingForceReplyId:Option[UUID]=None)

object ProcessorConfiguration extends ((String, Seq[String], String, MessageProcessor,Option[UUID])=>ProcessorConfiguration) {
  def apply(exchangeName:String, routingKey:String, outputRoutingKey:String, processor:MessageProcessor) = {
    new ProcessorConfiguration(
      exchangeName,
      Seq(routingKey),
      outputRoutingKey,
      processor)
  }
}