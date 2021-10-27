package com.gu.multimedia.mxscopy.streamcomponents

import akka.stream.scaladsl.GraphDSL

import java.security.MessageDigest
import akka.stream.{Attributes, Graph, Inlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, GraphStageLogic, GraphStageWithMaterializedValue}
import akka.util.ByteString
import org.slf4j.LoggerFactory
import org.apache.commons.codec.binary.Hex

import scala.concurrent.{Future, Promise}

/**
  * an akka streams sink that receives a ByteString then materializes the checksum value (as a hex string) at the end
  * of the stream
  * @param algorithm MessageDigest algorithm to use. This must be supported by `java.security.MessageDigest`. Defaults to "md5".
  */
class ChecksumSink(algorithm:String="md5") extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Option[String]]] {
  private final val in:Inlet[ByteString] = Inlet.create("MD5ChecksumSink.in")

  override def shape: SinkShape[ByteString] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Option[String]]) = {
    val completionPromise = Promise[Option[String]]()

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)
      private val md5Instance = MessageDigest.getInstance(algorithm)

      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          md5Instance.update(elem.toArray)
          pull(in)
        }
      })

      override def preStart(): Unit = {
        logger.info(s"Calculating checksum with algorithm $algorithm")
        pull(in)
      }

      override def postStop(): Unit = {
        val str = Hex.encodeHexString(md5Instance.digest())
        completionPromise.success(Some(str))
      }
    }
    (logic, completionPromise.future)
  }
}

object ChecksumSink {
  def apply: Graph[SinkShape[ByteString], Future[Option[String]]] = {
    val fac = new ChecksumSink()
    GraphDSL.createGraph(fac) { implicit builder =>
      sink => sink
    }
  }
}