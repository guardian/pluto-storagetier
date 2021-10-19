package streamcomponents

import java.nio.ByteBuffer

import akka.stream.stage.{AbstractInHandler, GraphStageLogic, GraphStageWithMaterializedValue}
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.util.ByteString
import com.om.mxs.client.japi.{AccessOption, MxsObject, SeekableByteChannel}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/**
  * writes the incoming ByteString stream to a previously obtained MXS file.
  * Does not write any metadata.
  * On stream completion, materializes a Long value containing the number of bytes written
  * @param mxsFile MxsObject object representing the file to write. This will be created.
  */
class TestingMatrixStoreFileSink(mxsFile:MxsObject) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Long]]{
  private final val in:Inlet[ByteString] = Inlet.create("TestingMatrixStoreFileSink.in")

  override def shape: SinkShape[ByteString] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {
    val completionPromise = Promise[Long]

    val logic:GraphStageLogic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)
      private var ctr:Long = 0

      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          val byteArr = grab(in).toArray
          ctr+=byteArr.length
          pull(in)
        }
      })

      override def preStart(): Unit = {
        try {
          logger.info(s"Mock sink would write to ${mxsFile.getId}...")
          pull(in)
        } catch {
          case err:Throwable=>
            logger.error(s"Could not set up MXS file to write to: ", err)
            failStage(err)
        }
      }

      override def postStop(): Unit = {
        completionPromise.success(ctr)
      }
    }
    (logic, completionPromise.future)
  }
}
