package com.gu.multimedia.storagetier.framework

import io.circe.Json

import scala.concurrent.Future

/**
 * this trait represents the protocol which must be implemented by any message processor class.
 * In order to use it, simply subclass the trait in your class and implement the `handleMessage` method.
 * See the docs on that method for details.
 * Then, you can pass it to the MessageProcessingFramework via:
 *   val config = Seq(
        ProcessorConfiguration(
          "test-input-exchange",
          "#",
          new MyMessageProcessorSubclass(subclass-constructor-args)
        )
      )
     MessageProcessingFramework(...args..., config) match {
        case Left(err)=> logger.error("the configuration is invalid, you have duplicate exchanges")
        case Right(framework)=> framework.run()
     }

   It is expected that there is only one ProcessorConfiguration for a given exchange, with a sufficiently general
   routing key applied to it.  So, don't use the raw constructor use the companion object (as above). This validates
   that the list of ProcessorConfigurations you give it do indeed have only one per exchange or it returns a Left
 */
trait MessageProcessor {
  /**
   * Override this method in your subclass to handle an incoming message
   * @param routingKey the routing key of the message as received from the broker.
   * @param msg the message body, as a circe Json object. You can unmarshal this into a case class by
   *            using msg.as[CaseClassFormat]
   * @return You need to return Left() with a descriptive error string if the message could not be processed, or Right
   *         with a circe Json body (can be done with caseClassInstance.noSpaces) containing a message body to send
   *         to our exchange with details of the completed operation
   */
  def handleMessage(routingKey:String, msg:Json):Future[Either[String,Json]]
}

