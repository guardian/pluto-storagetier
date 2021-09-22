package com.gu.multimedia.storagetier.framework

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.impl.AMQBasicProperties
import com.rabbitmq.client.{AMQP, Channel, Connection, ConnectionFactory, Envelope}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.syntax._
import io.circe.generic.auto._

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class MessageProcessingFrameworkSpec extends Specification with Mockito {
  "MessageProcessingFramework" should {
    "initialise the connection and queue" in {
      val mockRmqChannel = mock[Channel]
      val mockRmqConnection = mock[Connection]
      mockRmqConnection.createChannel() returns mockRmqChannel
      val mockRmqFactory = mock[ConnectionFactory]
      mockRmqFactory.newConnection() returns mockRmqConnection

      implicit val connectionFactoryProvider:ConnectionFactoryProvider = mock[ConnectionFactoryProvider]
      connectionFactoryProvider.get() returns mockRmqFactory

      val mockedMessageProcessor = mock[MessageProcessor]

      val handlers = Seq(
        ProcessorConfiguration("some-exchange","input.routing.key", mockedMessageProcessor)
      )

      val f = new MessageProcessingFramework("input-queue",
        "output-exchg",
        "some.routing.key",
        "retry-exchg",
        "failed-exchg",
        "failed-q",
        handlers)

      f.run()
      there was one(mockRmqFactory).newConnection()
      there was one(mockRmqConnection).createChannel()
      there was one(mockRmqChannel).queueDeclare("input-queue", true, false, false,Map[String,AnyRef]().asJava)
      there was one(mockRmqChannel).queueBind("input-queue","some-exchange","input.routing.key")
      there was one(mockRmqChannel).basicConsume("input-queue", false, f.MsgConsumer)

    }
  }

  "MessageProcessingFramework.MsgConsumer.HandleDelivery" should {
    "drop a message that does not parse to json" in {
      val mockRmqChannel = mock[Channel]
      val mockRmqConnection = mock[Connection]
      mockRmqConnection.createChannel() returns mockRmqChannel
      val mockRmqFactory = mock[ConnectionFactory]
      mockRmqFactory.newConnection() returns mockRmqConnection

      implicit val connectionFactoryProvider:ConnectionFactoryProvider = mock[ConnectionFactoryProvider]
      connectionFactoryProvider.get() returns mockRmqFactory

      val mockedMessageProcessor = mock[MessageProcessor]

      val handlers = Seq(
        ProcessorConfiguration("some-exchange","input.routing.key", mockedMessageProcessor)
      )

      val f = new MessageProcessingFramework("input-queue",
        "output-exchg",
        "some.routing.key",
        "retry-exchg",
        "failed-exchg",
        "failed-q",
        handlers)

      val envelope = new Envelope(12345678L, false, "fake-exchange","some.routing.key")
      val msgProps = new BasicProperties.Builder().messageId("fake-message-id").build()
      val msgBytes = "{something: \"is not json}".getBytes
      f.MsgConsumer.handleDelivery("test-consumer",envelope, msgProps, msgBytes)

      there was no(mockRmqChannel).basicAck(any,any)
      there was one(mockRmqChannel).basicNack(12345678L, false, false)
      there was no(mockRmqChannel).basicPublish(org.mockito.ArgumentMatchers.eq("retry-exchg"),any,any,any)
      there was one(mockRmqChannel).basicPublish(
        org.mockito.ArgumentMatchers.eq("failed-exchg"),
        org.mockito.ArgumentMatchers.eq("some.routing.key"),
        any,
        any
      )
      there was no(mockedMessageProcessor).handleMessage(any, any)
    }

    "reject and retry a valid message if there is no configured handler" in {
      val mockRmqChannel = mock[Channel]
      val mockRmqConnection = mock[Connection]
      mockRmqConnection.createChannel() returns mockRmqChannel
      val mockRmqFactory = mock[ConnectionFactory]
      mockRmqFactory.newConnection() returns mockRmqConnection

      implicit val connectionFactoryProvider:ConnectionFactoryProvider = mock[ConnectionFactoryProvider]
      connectionFactoryProvider.get() returns mockRmqFactory

      val mockedMessageProcessor = mock[MessageProcessor]

      val handlers = Seq(
        ProcessorConfiguration("some-exchange","input.routing.key", mockedMessageProcessor)
      )

      val f = new MessageProcessingFramework("input-queue",
        "output-exchg",
        "some.routing.key",
        "retry-exchg",
        "failed-exchg",
        "failed-q",
        handlers)

      val envelope = new Envelope(12345678L, false, "fake-exchange","some.routing.key")
      val msgProps = new BasicProperties.Builder().messageId("fake-message-id").build()
      val msgBytes = "{\"key\":\"value\"}".getBytes
      f.MsgConsumer.handleDelivery("test-consumer",envelope, msgProps, msgBytes)

      val expectedProperties = new BasicProperties.Builder()
        .contentType("application/json")
        .expiration("2000")
        .headers(Map("retry-count"->1.asInstanceOf[AnyRef]).asJava)
        .build()

      there was one(mockRmqChannel).basicAck(12345678L, false)
      there was no(mockRmqChannel).basicNack(any,any,any)
      there was one(mockRmqChannel).basicPublish(
        org.mockito.ArgumentMatchers.eq("retry-exchg"),
        org.mockito.ArgumentMatchers.eq("some.routing.key"),
        org.mockito.ArgumentMatchers.eq(false),
        org.mockito.ArgumentMatchers.eq(expectedProperties),
        org.mockito.ArgumentMatchers.eq(msgBytes)
      )
      there was no(mockRmqChannel).basicPublish(
        org.mockito.ArgumentMatchers.eq("failed-exchg"),
        any,
        any,
        any
      )
      there was no(mockedMessageProcessor).handleMessage(any, any)
    }

    "pass a valid message to the configured handler and return a successful reply" in {
      val mockRmqChannel = mock[Channel]
      val mockRmqConnection = mock[Connection]
      mockRmqConnection.createChannel() returns mockRmqChannel
      val mockRmqFactory = mock[ConnectionFactory]
      mockRmqFactory.newConnection() returns mockRmqConnection

      implicit val connectionFactoryProvider:ConnectionFactoryProvider = mock[ConnectionFactoryProvider]
      connectionFactoryProvider.get() returns mockRmqFactory

      val mockedMessageProcessor = mock[MessageProcessor]
      val responseMsg = Map("status"->"ok").asJson
      mockedMessageProcessor.handleMessage(any, any) returns Future(Right(responseMsg))
      val handlers = Seq(
        ProcessorConfiguration("some-exchange","input.routing.key", mockedMessageProcessor)
      )

      val f = new MessageProcessingFramework("input-queue",
        "output-exchg",
        "some.routing.key",
        "retry-exchg",
        "failed-exchg",
        "failed-q",
        handlers)

      val envelope = new Envelope(12345678L, false, "some-exchange","some.routing.key")
      val msgProps = new BasicProperties.Builder().messageId("fake-message-id").build()
      val msgBytes = "{\"key\":\"value\"}".getBytes
      f.MsgConsumer.handleDelivery("test-consumer",envelope, msgProps, msgBytes)

      val expectedProperties = new AMQP.BasicProperties.Builder()
        .contentType("application/octet-stream")
        .build()

      there was one(mockRmqChannel).basicAck(12345678L, false)
      there was no(mockRmqChannel).basicNack(any,any,any)
      there was one(mockRmqChannel).basicPublish(
        org.mockito.ArgumentMatchers.eq("output-exchg"),
        org.mockito.ArgumentMatchers.eq("some.routing.key"),
        org.mockito.ArgumentMatchers.eq(expectedProperties),
        org.mockito.ArgumentMatchers.eq(responseMsg.noSpaces.getBytes)
      )
      there was no(mockRmqChannel).basicPublish(
        org.mockito.ArgumentMatchers.eq("failed-exchg"),
        any,
        any,
        any
      )
      there was no(mockRmqChannel).basicPublish(
        org.mockito.ArgumentMatchers.eq("retry-exchg"),
        any,
        any,
        any
      )
      there was one(mockedMessageProcessor).handleMessage(any, any)
    }
  }

  "reject and retry a valid message if the handler indicates an error" in {
    val mockRmqChannel = mock[Channel]
    val mockRmqConnection = mock[Connection]
    mockRmqConnection.createChannel() returns mockRmqChannel
    val mockRmqFactory = mock[ConnectionFactory]
    mockRmqFactory.newConnection() returns mockRmqConnection

    implicit val connectionFactoryProvider:ConnectionFactoryProvider = mock[ConnectionFactoryProvider]
    connectionFactoryProvider.get() returns mockRmqFactory

    val mockedMessageProcessor = mock[MessageProcessor]
    mockedMessageProcessor.handleMessage(any, any) returns Future(Left("test error"))

    val handlers = Seq(
      ProcessorConfiguration("some-exchange","input.routing.key", mockedMessageProcessor)
    )

    val f = new MessageProcessingFramework("input-queue",
      "output-exchg",
      "some.routing.key",
      "retry-exchg",
      "failed-exchg",
      "failed-q",
      handlers)

    val envelope = new Envelope(12345678L, false, "fake-exchange","some.routing.key")
    val msgProps = new BasicProperties.Builder().messageId("fake-message-id").build()
    val msgBytes = "{\"key\":\"value\"}".getBytes
    f.MsgConsumer.handleDelivery("test-consumer",envelope, msgProps, msgBytes)

    val expectedProperties = new BasicProperties.Builder()
      .contentType("application/json")
      .expiration("2000")
      .headers(Map("retry-count"->1.asInstanceOf[AnyRef]).asJava)
      .build()

    there was one(mockRmqChannel).basicAck(12345678L, false)
    there was no(mockRmqChannel).basicNack(any,any,any)
    there was one(mockRmqChannel).basicPublish(
      org.mockito.ArgumentMatchers.eq("retry-exchg"),
      org.mockito.ArgumentMatchers.eq("some.routing.key"),
      org.mockito.ArgumentMatchers.eq(false),
      org.mockito.ArgumentMatchers.eq(expectedProperties),
      org.mockito.ArgumentMatchers.eq(msgBytes)
    )
    there was no(mockRmqChannel).basicPublish(
      org.mockito.ArgumentMatchers.eq("failed-exchg"),
      any,
      any,
      any
    )
    there was no(mockedMessageProcessor).handleMessage(any, any)
  }
}
