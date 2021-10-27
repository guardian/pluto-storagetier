package com.gu.multimedia.mxscopy.streamcomponents

import java.io.InputStream
import java.nio.{BufferUnderflowException, ByteBuffer}

import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.util.ByteString
import com.om.mxs.client.japi.{MatrixStore, MxsObject, UserInfo, Vault}
import org.slf4j.LoggerFactory


class TestingMatrixStoreFileSource(forBuffer:ByteBuffer, bufferSize:Int=10) extends GraphStage[SourceShape[ByteString]]{
  private final val out:Outlet[ByteString] = Outlet.create("TestingMatrixStoreFileSource.out")

  override def shape: SourceShape[ByteString] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        val maybeBytes = try {
          val bytes = new Array[Byte](bufferSize)
          forBuffer.get(bytes)
          Some(bytes)
        } catch {
          case _: BufferUnderflowException =>
            if (forBuffer.hasRemaining) {
              val remainingBytes = new Array[Byte](forBuffer.remaining())
              forBuffer.get(remainingBytes)
              Some(remainingBytes)
            } else {
              None
            }
        }

        maybeBytes match {
          case None =>
            logger.info(s"Fake MXS read completed")
            complete(out)
          case Some(bytes) =>
            logger.debug(s"Pushing ${bytes.length} bytes into the stream...")

            push(out, ByteString(bytes))
        }

      }
    })
  }
}
