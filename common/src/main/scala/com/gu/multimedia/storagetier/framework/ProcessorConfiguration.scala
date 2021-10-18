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
                                  outputRoutingKeys:Seq[String],
                                  processor:MessageProcessor,
                                  testingForceReplyId:Option[UUID]=None)

object ProcessorConfiguration extends ((String, Seq[String], Seq[String], MessageProcessor,Option[UUID])=>ProcessorConfiguration) {
  def apply(exchangeName:String, routingKey:Seq[String], outputRoutingKey:Seq[String], processor:MessageProcessor) = {
    if(routingKey.length!=outputRoutingKey.length) throw new RuntimeException(s"Processor configuration for $exchangeName is invalid, need equal input/output routing keys")
    new ProcessorConfiguration(
      exchangeName,
      routingKey,
      outputRoutingKey,
      processor
    )
  }

  def apply(exchangeName:String, routingKey:String, outputRoutingKey:String, processor:MessageProcessor) = {
    new ProcessorConfiguration(
      exchangeName,
      Seq(routingKey),
      Seq(outputRoutingKey),
      processor)
  }
}