package com.gu.multimedia.storagetier.framework

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.impl.{CredentialsProvider, DefaultCredentialsProvider}
import com.rabbitmq.client.{AMQP, Channel, Connection, Consumer, Envelope, LongString, ShutdownSignalException}
import io.circe.Json

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import org.slf4j.{LoggerFactory, MDC}

import scala.concurrent.duration._
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.UUID
import scala.collection.mutable

/**
 * This class forms the basis of the message processing framework.
 * In order to initialise it, you must pass in a list of ProcessorConfiguration instances to define the exchanges
 * you want to listen to and the handler to send it those messages to.
 *
 * @param ingest_queue_name name of the queue that will be subscribed to all of the incoming exchanges. This should be shared
 *                          between instances of the app.
 * @param output_exchange_name name of the exchange that will receive success messages
 * @param routingKeyForSend routing key to use for success messages
 * @param retryExchangeName name of the exchange that is used for dead-letter retries
 * @param failedExchangeName name of the dead-letter exchange that will be used for non-retryable messages
 * @param handlers handler configuration
 * @param maximumDelayTime maximum time to set for a delivery retry, in milliseconds. Defaults to 120,000 (=2min)
 * @param connectionFactoryProvider implicitly provided ConnectionFactoryProvider, which allows us to get a rabbitmq instnace
 */
class MessageProcessingFramework (ingest_queue_name:String,
                                  output_exchange_name:String,
                                  retryExchangeName:String,
                                  failedExchangeName:String,
                                  failedQueueName:String,
                                  handlers:Seq[ProcessorConfiguration],
                                  maximumDelayTime:Int=120000,
                                  maximumRetryLimit:Int=200)
                                 (channel:Channel, conn:Connection)(implicit ec:ExecutionContext){
  private val logger = LoggerFactory.getLogger(getClass)
  private val cs = Charset.forName("UTF-8")
  private val completionPromise = Promise[Unit]
  private val retryInputExchangeName = retryExchangeName + "-r"

  import AMQPBasicPropertiesExtensions._

  /**
   * handle the java api protocol for receiving messages
   */
  object MsgConsumer extends Consumer {
    override def handleConsumeOk(consumerTag: String): Unit = {
      logger.info("Consumer started up")
    }

    override def handleCancelOk(consumerTag: String): Unit = {
      logger.info("Consumer was cancelled internally, exiting")
      completionPromise.complete(Success())
    }

    override def handleCancel(consumerTag: String): Unit = {
      logger.info("Consumer was cancelled, exiting")
      completionPromise.complete(Success())
    }

    override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit = {
      logger.info(s"Broker connection is shutting down due to ${sig.getMessage}")
      if(sig.getMessage.contains("clean connection shutdown")) {
        completionPromise.complete(Success())
      } else {
        completionPromise.complete(Failure(sig))
      }
    }

    override def handleRecoverOk(consumerTag: String): Unit = {
      //we don't use basic.recover at present
    }

    /**
     * wraps the circe parse method to change the Left type into a string,
     * in order to make it easier to compose
     * @param stringContent the string content to parse
     * @return either a string containing an error or the parsed io.circe.Json object
     */
    private def wrappedParse(stringContent:String):Either[String,Json] = {
      io.circe.parser.parse(stringContent)
    }.left.map(_.getMessage())

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
      val retryAttempt = Try {
        properties.getHeaders.asScala.getOrElse("retry-count",0).asInstanceOf[Int]
      }.toOption.getOrElse(0)

      MDC.put("msgId", Option(properties.getMessageId).getOrElse(""))
      MDC.put("retryAttempt", retryAttempt.toString)
      MDC.put("routingKey", Option(envelope.getRoutingKey).getOrElse(""))
      MDC.put("exchange", Option(envelope.getExchange).getOrElse(""))
      MDC.put("isRedeliver", Option(envelope.isRedeliver).getOrElse(false).toString)

      val matchingConfigurations =
        if(envelope.getExchange==retryInputExchangeName) {  //if the message came from the retry exchange, then look up the original exchange and use that

          logger.debug(s"Message ${properties.getMessageId} is a retry, on attempt ${retryAttempt}")
          logger.debug(s"Message headers: ${properties.getHeaders}")

          if(retryAttempt>=maximumRetryLimit) {
            permanentlyRejectMessage(envelope, properties, body, "Too many retries")
            return
          }
          properties.getHeader[LongString]("x-original-exchange") match {
            case Some(effectiveExchange)=>
              logger.debug(s"Original exchange is $effectiveExchange, routing to that processor")
              handlers.filter(_.exchangeName==effectiveExchange.toString)
            case None=>
              logger.error(s"Could not determine the original exchange for retried message ${properties.getMessageId}")
              Seq()
          }
        } else {  //otherwise just use the exchange given by the envelope
          handlers.filter(_.exchangeName==envelope.getExchange)
        }

      logger.debug(s"${matchingConfigurations.length} processors matched, will use the first")

      val completionFuture = convertToUTFString(body).flatMap(wrappedParse) match {
        case Left(err)=>
          Future.fromTry(permanentlyRejectMessage(envelope, properties, body, err))
        case Right(msg)=>
          if(matchingConfigurations.isEmpty) {
            logger.error(s"No processors are configured to handle messages from ${envelope.getExchange}")
            Future.fromTry(rejectMessage(envelope, Some(properties), msg))
          } else {
            val targetConfig = matchingConfigurations.head
            targetConfig.processor.handleMessage(envelope.getRoutingKey, msg).map({
              case Left(errDesc)=>
                logger.warn(s"MsgID ${properties.getMessageId} Retryable failure: \"$errDesc\"")
                rejectMessage(envelope, Option(properties), msg)
              case Right(returnValue)=>
                RoutingKeyMatcher.findMatchingIndex(targetConfig.routingKey, envelope.getRoutingKey) match {
                  case Some(inputKeyIndex) =>
                    logger.debug(s"actual routing key ${envelope.getRoutingKey} matches source $inputKeyIndex")
                    confirmMessage(envelope.getDeliveryTag,
                      targetConfig.outputRoutingKeys(inputKeyIndex),
                      Option(properties).flatMap(p => Option(p.getMessageId)),
                      returnValue,
                      targetConfig.testingForceReplyId)
                    logger.info(s"MsgID ${properties.getMessageId} Successfully handled message")
                  case None =>
                    logger.error(s"No routing key input spec matched the actual key of ${envelope.getRoutingKey}! Configured input specs were ${targetConfig.routingKey}. Outputting to ${targetConfig.outputRoutingKeys.head}")
                    confirmMessage(envelope.getDeliveryTag,
                      targetConfig.outputRoutingKeys.head,
                      Option(properties).flatMap(p => Option(p.getMessageId)),
                      returnValue,
                      targetConfig.testingForceReplyId)
                }
            }).recover({
              case err:SilentDropMessage=>
                logger.info(s"Dropping message with id ${properties.getMessageId}: ${err.getMessage}")
                channel.basicAck(envelope.getDeliveryTag, false)
              case err:java.io.IOException=>  //matrixstore errors get converted into IOException, which is not very useful for us :(
                logger.error(s"MsgId ${properties.getMessageId} - Got java.io.IOException while trying to handle the message, retrying: ${err.getMessage}")
                if(err.getMessage.contains("failed to retrieve work permit")) {
                  logger.error("Got work permit error, this indicates either MatrixStore busy or failure. Waiting for 4mins to reduce load.")
                  Thread.sleep(240000)  //=240 seconds = 4mins
                }
                rejectMessage(envelope, Option(properties), msg)
              case err:Throwable=>
                logger.error(s"MsgID ${properties.getMessageId} - Got an exception ${err.getClass.getCanonicalName} while trying to handle the message: ${err.getMessage}", err)
                permanentlyRejectMessage(envelope, properties, body, err.getMessage)
            })
          }
      }
      //Why are we using Await here, when you are not meant to use it in Scala production code? Well, the
      //process seems to be executing multiple messages in parallel, which is not what we want and is causing issues
      //when under load.  So, to prevent the rabbitmq library from sending us another message immediately we block the thread
      //here until we have definitively handled it.
      //The Try here _should_ never fail as exceptions are handled in the block above.
      Try { Await.ready(completionFuture, 60.minutes) } match {
        case Success(_)=>
          MDC.clear()
        case Failure(err)=>
          logger.error(s"HandleDeliver failed for message id ${properties.getMessageId}: ${err.getMessage}", err)
          MDC.clear()
      }
    }
  }

  /**
   * reliably decode a raw byte array into a UTF-8 string.
   * @param raw raw byte array
   * @return either a Right with the decoded string, or a Left with an error message
   */
  private def convertToUTFString(raw:Array[Byte]):Either[String,String] = {
    try {
      val buf = ByteBuffer.wrap(raw)
      Right(cs.decode(buf).rewind().toString)
    } catch {
      case err:Throwable=>
        Left(err.getMessage)
    }
  }


  /**
   * internal method.
   * Confirm that an event took place, by acknowleging processing of the original message and sending a message
   * to our output exchange
   * @param deliveryTag the delivery tag of the incoming message that was successfully processed
   * @param confirmationData a circe Json body of content to send out onto our exchange
   * @return
   */
  private def confirmMessage(deliveryTag: Long, routingKeyForSend:String, previousMessageId:Option[String], confirmationData:MessageProcessorReturnValue, newMessageId:Option[UUID]=None) = Try {
    val stringContent = confirmationData.content.noSpaces

    channel.basicAck(deliveryTag, false)
    val msgProps = new AMQP.BasicProperties.Builder()
      .contentType("application/json")
      .contentEncoding("UTF-8")
      .messageId(newMessageId.getOrElse(UUID.randomUUID()).toString)
      .headers(Map("x-in-response-to"->previousMessageId.orNull.asInstanceOf[AnyRef]).asJava)
      .build()

    confirmationData.additionalDestinations.foreach(dest=>{
      channel.basicPublish(dest.outputExchange, dest.routingKey, msgProps, stringContent.getBytes(cs))
    })
    channel.basicPublish(output_exchange_name, routingKeyForSend + ".success", msgProps, stringContent.getBytes(cs))
  }

  /**
   * internal method.  Handle a permanent failure by forwarding the message onto the dead-letter queue with updated headers
   * to indicate where the message came from and why it was rejected.  No-ack it from the original queue without a re-send.
   * @param envelope Envelope object from the server
   * @param properties AMQP.BasicProperties from the server
   * @param body raw message content, as a byte array
   * @param err descriptive error string, which is set as the "error" header in the rejected message
   * @return a Try with no value if the operation worked or an exception if it failed.
   */
  private def permanentlyRejectMessage(envelope: Envelope, properties:AMQP.BasicProperties, body:Array[Byte], err:String) = {
    //drop the dodgy message and send it directly to the DLX
    channel.basicNack(envelope.getDeliveryTag, false, false)
    logger.error(s"Message with ID ${properties.getMessageId} is invalid and will be dropped.")
    logger.error(s"${properties.getMessageId}: invalid content was ${convertToUTFString(body)}")
    logger.error(s"${properties.getMessageId}: error was $err")
    val originalHeaders = Option(properties.getHeaders).map(_.asScala).getOrElse(mutable.Map())

    val updatedHeaders = originalHeaders ++ mutable.Map(
      "error"->err,
      "x-original-exchange"->envelope.getExchange,
      "x-original-routing-key"->envelope.getRoutingKey
    )

    val dlqProps = new AMQP.BasicProperties.Builder()
      .contentType("application/octet-stream")
      .messageId(properties.getMessageId)
      .headers(updatedHeaders.asJava)
      .build()

    Try { channel.basicPublish(failedExchangeName, envelope.getRoutingKey, dlqProps, body) }
  }

  /**
   * internal method.
   * Performs message rejection by pushing it onto a retry-queue unless there have been too many retries.
   * Exponential backoff is implemented through a deadletter queue mechanism - the message is re-sent here onto the
   * "retry exchange" with an "expiration" property equal to the delay time. The "retry exchange" then forwards on to
   * a "retry queue" which holds the message until the "expiration" time is up, then dead-letters it onto the "retry input
   * exchange".  The "retry input exchange" is bound to a "retry input queue" which receives the message.  We subscribe to
   * the "retry input queue" so an instance will the receive the message.
   * We set some custom headers on the message so we can tell which exchange it was originally sent from and route it
   * internally to the correct processor
   * Note that 'rejected' messages are still ACK'd as they are sent on to a delay exchange
   * @param envelope message Envelope
   * @param properties message Properties
   * @param content parsed message content, as a circe.Json type
   * @return Success with unit value if it worked, or a Failure with exception if it didn't
   */
  private def rejectMessage(envelope: Envelope, properties:Option[AMQP.BasicProperties], content:Json) = Try {
    //handle either properties or headers being null
    val maybeHeaders = for {
      props <- properties
      headers <- Option(props.getHeaders).map(_.asScala)
    } yield headers
    val originalMsgHeaders = maybeHeaders.getOrElse(Map[String, AnyRef]())

    val nextRetryCount = originalMsgHeaders.getOrElse("retry-count",0) match {
      case intValue:Int=>
        logger.info(s"Previous retry of message ${properties.map(_.getMessageId)} is $intValue")
        intValue+1
      case _=>
        logger.warn(s"Got unexpected value type for retry-count header on message id ${properties.map(_.getMessageId)}, resetting to 1")
        1
    }
    val delayTime = List(math.pow(2, nextRetryCount)*1000, maximumDelayTime).min.toInt
    logger.debug(s"delayTime is $delayTime")
    val originalExchange = properties
      .flatMap(_.getHeader[LongString]("x-original-exchange"))
      .map(_.toString)
      .getOrElse(envelope.getExchange)

    val updatedMsgHeaders = originalMsgHeaders ++ Map(
      "retry-count"->nextRetryCount.asInstanceOf[AnyRef],
      "x-original-exchange"->originalExchange.asInstanceOf[AnyRef],
      "x-original-routing-key"->envelope.getRoutingKey.asInstanceOf[AnyRef]
    )

    val newProps = new BasicProperties.Builder()
      .contentType("application/json")
      .expiration(delayTime.toString)
      .headers(updatedMsgHeaders.asJava)
      .messageId(properties.map(_.getMessageId).getOrElse(UUID.randomUUID().toString))
      .build()

    channel.basicPublish(retryExchangeName, envelope.getRoutingKey, false, newProps, content.noSpaces.getBytes(cs))
    channel.basicAck(envelope.getDeliveryTag, false)
  }

  private def simpleQueueDeclare(queueName:String) = channel.queueDeclare(queueName, true, false, false,Map[String, AnyRef]().asJava )
  /**
   * Kick off the framework.  This returns a future which should only resolve when the framework terminates.
   * @return
   */
  def run() = {
    try {
      //output exchange where we send our completion notifications
      channel.exchangeDeclare(output_exchange_name, "topic",
        true,
        false,
        Map("x-dead-letter-exchange" -> failedExchangeName.asInstanceOf[AnyRef]).asJava
      )

      //dead-letter queue for permanent failures
      channel.exchangeDeclare(failedExchangeName,
        "topic",
        true)
      simpleQueueDeclare(failedQueueName)
      channel.queueBind(failedQueueName, failedExchangeName, "#")

      //messages posted to the retryExchange are routed to a queue where they are delayed by the TTL provided on
      //the message and are then sent back to the retryInputExchange
      channel.exchangeDeclare(retryExchangeName, "topic", false)

      //messages come onto the RetryInputExchange and we pick them up and re-process them from there
      channel.exchangeDeclare(retryInputExchangeName, "topic", false)

      //link the retryExchange back to the retryInputExchange with a queue named after the retryInputExchange.
      // The message is received from retryExchange then expires
      // after its provided TTL which then triggers it to be "dead-lettered" back onto the retryInputExchange
      channel.queueDeclare(retryInputExchangeName,
        true,
        false,
        false,
        Map[String,AnyRef](
          "x-dead-letter-exchange"->retryInputExchangeName.asInstanceOf[AnyRef],
          "x-message-ttl"->maximumDelayTime.asInstanceOf[AnyRef]
        ).asJava
      )
      channel.queueBind(retryInputExchangeName, retryExchangeName, "#")

      //we declare a single queue that receives all the messages we are interested in, and bind it to the retry input exchange
      channel.queueDeclare(ingest_queue_name,
        true,
        false,
        false,
        Map[String,AnyRef](
          "x-dead-letter-exchange"->failedExchangeName.asInstanceOf[AnyRef],
        ).asJava
      )

      channel.queueBind(ingest_queue_name, retryInputExchangeName, "#")

      //now we also bind it to all of the exchanges that are listed in our configuration
      handlers.foreach(conf => {
        conf.routingKey.foreach(routingKey=> {
          channel.queueBind(ingest_queue_name, conf.exchangeName, routingKey)
        })
      })

      channel.basicQos(1, true)
      channel.basicConsume(ingest_queue_name, false, MsgConsumer)
    } catch {
      case err:Throwable=>completionPromise.failure(err)
    }
    completionPromise.future
  }

  /**
   * shuts down the broker connection
   * @param timeout maximum time to wait for shutdown, in milliseconds. Default is 30,000 (=30s)
   * @return a Success with unit value if it worked, or a Failure if there was a problem
   */
  def terminate(timeout:Int=30000) = Try {
    conn.close(timeout)
  }
}

object MessageProcessingFramework {
  private val logger = LoggerFactory.getLogger(getClass)

  private def initialiseRabbitMQ(implicit connectionFactoryProvider: ConnectionFactoryProvider) = Try {
    val factory = connectionFactoryProvider.get()
    val rmqHost = sys.env.getOrElse("RABBITMQ_HOST", "localhost")
    val rmqVhost = sys.env.getOrElse("RABBITMQ_VHOST","pluto-ng")

    factory.setHost(rmqHost)
    factory.setVirtualHost(rmqVhost)
    factory.setCredentialsProvider(new DefaultCredentialsProvider(
      sys.env.getOrElse("RABBITMQ_USER","storagetier"),
      sys.env.getOrElse("RABBITMQ_PASSWORD","password")
    ))

    val conn = factory.newConnection()
    (conn.createChannel(), conn)
  }

  def apply(ingest_queue_name:String,
            output_exchange_name:String,
            routingKeyForSend: String,
            retryExchangeName:String,
            failedExchangeName:String,
            failedQueueName:String,
            handlers:Seq[ProcessorConfiguration])
           (implicit connectionFactoryProvider: ConnectionFactoryProvider, ec:ExecutionContext) = {
    val exchangeNames = handlers.map(_.exchangeName)
    if(exchangeNames.distinct.length != exchangeNames.length) { // in this case there must be duplicates
      Left(s"You have ${exchangeNames.length-exchangeNames.distinct.length} duplicate exchange names in your configuration, that is not valid.")
    } else if(routingKeyForSend.endsWith(".")) {
      Left("output routing key cannot end with a .")
    } else {
      initialiseRabbitMQ match {
        case Failure(err) =>
          logger.error(s"Could not initialise RabbitMQ: ${err.getMessage}")
          Left(err.getMessage)
        case Success((channel, conn)) =>
          Right(
            new MessageProcessingFramework(ingest_queue_name,
              output_exchange_name,
              retryExchangeName,
              failedExchangeName,
              failedQueueName,
              handlers)(channel, conn)
          )
      }
    }
  }
}
