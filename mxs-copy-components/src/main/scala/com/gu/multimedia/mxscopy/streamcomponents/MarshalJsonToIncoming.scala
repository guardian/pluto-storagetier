package com.gu.multimedia.mxscopy.streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import io.circe.Json
import com.gu.multimedia.mxscopy.models.IncomingListEntry
import org.slf4j.LoggerFactory
import io.circe.generic.auto._
import io.circe.syntax._

import scala.util.{Failure, Success}

class MarshalJsonToIncoming extends GraphStage[FlowShape[Json, IncomingListEntry]] {
  private final val in:Inlet[Json] = Inlet.create("MarshalJsonToIncoming.in")
  private final val out:Outlet[IncomingListEntry] = Outlet.create("MarshalJsonToIncoming.out")

  override def shape: FlowShape[Json, IncomingListEntry] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val json = grab(in)

        IncomingListEntry.fromJson(json) match {
          case Success(entry)=>push(out,entry)
          case Failure(err)=>
            logger.error(s"Could not marshal json into domain object: $err")
            fail(out, err)
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }

}
