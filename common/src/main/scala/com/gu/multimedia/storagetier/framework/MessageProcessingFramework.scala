package com.gu.multimedia.storagetier.framework

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.impl.{CredentialsProvider, DefaultCredentialsProvider}
import com.rabbitmq.client.{AMQP, Consumer, Envelope, ShutdownSignalException}
import io.circe.Json

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits._
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.nio.charset.Charset
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
 * @param connectionFactoryProvider implicitly provided ConnectionFactoryProvider, which allows us to get a rabbitmq instnace
 */
class MessageProcessingFramework (ingest_queue_name:String,
                                  output_exchange_name:String,
                                  routingKeyForSend: String,
                                  retryExchangeName:String,
                                  failedExchangeName:String,
                                  handlers:Seq[ProcessorConfiguration])
                                 (implicit connectionFactoryProvider: ConnectionFactoryProvider){
  private val logger = LoggerFactory.getLogger(getClass)
  private lazy val rmqHost = sys.env.getOrElse("RABBITMQ_HOST", "localhost")
  private val factory = connectionFactoryProvider.get()
  private val cs = Charset.forName("UTF-8")

  factory.setHost(rmqHost)
  factory.setCredentialsProvider(new DefaultCredentialsProvider(
    sys.env.getOrElse("RABBITMQ_USER","storagetier"),
    sys.env.getOrElse("RABBITMQ_PASSWORD","password")
  ))

  private val conn = factory.newConnection()
  private val channel = conn.createChannel()

  /**
   * handle the java api protocol for receiving messages
   */
  object MsgConsumer extends Consumer {
    override def handleConsumeOk(consumerTag: String): Unit = {
      logger.info("Consumer started up")
    }

    override def handleCancelOk(consumerTag: String): Unit = {
      logger.info("Consumer was cancelled internally, exiting")
    }

    override def handleCancel(consumerTag: String): Unit = {
      logger.info("Consumer was cancelled, exiting")
    }

    override def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit = {
      logger.info(s"Broker connection is shutting down due to ${sig.getMessage}")
    }

    override def handleRecoverOk(consumerTag: String): Unit = {
      //we don't use basic.recover at present
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
     * wraps the circe parse method to change the Left type into a string,
     * in order to make it easier to compose
     * @param stringContent the string content to parse
     * @return either a string containing an error or the parsed io.circe.Json object
     */
    private def wrappedParse(stringContent:String):Either[String,Json] = {
      io.circe.parser.parse(stringContent)
    }.left.map(_.getMessage())

    override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
      val matchingExchanges = handlers.filter(_.exchangeName==envelope.getExchange)

      val result = convertToUTFString(body).flatMap(wrappedParse) match {
        case Left(err)=>
          //drop the dodgy message and send it directly to the DLX
          channel.basicNack(envelope.getDeliveryTag, false, false)
          logger.error(s"Message with ID ${properties.getMessageId} is invalid and will be dropped.")
          logger.error(s"${properties.getMessageId}: invalid content was ${convertToUTFString(body)}")
          logger.error(s"${properties.getMessageId}: error was $err")
          val originalHeaders = Option(properties.getHeaders).map(_.asScala).getOrElse(mutable.Map())
          val updatedHeaders = originalHeaders ++ mutable.Map("error"->err)
          val dlqProps = new AMQP.BasicProperties.Builder()
            .contentType("application/octet-stream")
            .messageId(properties.getMessageId)
            .headers(updatedHeaders.asJava)
            .build()

          Try { channel.basicPublish(failedExchangeName, envelope.getRoutingKey, dlqProps, body) }
        case Right(msg)=>
          if(matchingExchanges.isEmpty) {
            logger.error(s"No processors are configured to handle messages from ${envelope.getExchange}")
            rejectMessage(envelope, Some(properties), msg)
          } else {
            val targetProcessor = matchingExchanges.head.processor
                targetProcessor.handleMessage(msg) match {
                  case Left(errDesc)=>
                    rejectMessage(envelope, Option(properties), msg)
                  case Right(returnValue)=>
                    confirmMessage(envelope.getDeliveryTag, returnValue)
                }
            }
      }

      result match {
        case Success(_)=>
          logger.debug(s"${properties.getMessageId} Successfully handled message")
        case Failure(err)=>
          logger.error(s"${properties.getMessageId} - Got an exception while trying to handle the message: ${err.getMessage}", err)
      }
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
  private def confirmMessage(deliveryTag: Long, confirmationData:Json) = Try {
    val stringContent = confirmationData.noSpaces

    channel.basicAck(deliveryTag, false)
    val msgProps = new AMQP.BasicProperties.Builder()
      .contentType("application/octet-stream")
      .build()

    channel.basicPublish(output_exchange_name, routingKeyForSend, msgProps, stringContent.getBytes(cs))
  }

  /**
   * internal method.
   * Performs message rejection by pushing it onto a retry-queue unless there have been too many retries.
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
    val delayTime = List(math.pow(2, nextRetryCount), 60000).min
    val updatedMsgHeaders = originalMsgHeaders ++ Map("retry-count"->nextRetryCount.asInstanceOf[AnyRef])
    val newProps = new BasicProperties.Builder()
      .contentType("application/json")
      .expiration(delayTime.toString)
      .headers(updatedMsgHeaders.asJava)
      .build()

    channel.basicPublish(retryExchangeName, envelope.getRoutingKey, false, newProps, content.noSpaces.getBytes(cs))
    channel.basicAck(envelope.getDeliveryTag, false)
  }

  /**
   * Kick off the framework.  This returns a future which should only resolve when the framework terminates.
   * @return
   */
  def run() = Future {
    channel.queueDeclare(ingest_queue_name, true, false, false,Map[String,AnyRef]().asJava)

    handlers.foreach(conf=>{
      channel.queueBind(ingest_queue_name, conf.exchangeName, conf.routingKey)
    })

    channel.basicConsume(ingest_queue_name, false, MsgConsumer)
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
