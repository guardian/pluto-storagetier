package streamcomponents

import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import io.circe.Json
import org.slf4j.LoggerFactory

class ParseJsonEntry extends GraphStage[FlowShape[String, Json]] {
  private final val in:Inlet[String] = Inlet.create("ParseJsonEntry.in")
  private final val out:Outlet[Json] = Outlet.create("ParseJsonFactory.out")

  override def shape: FlowShape[String, Json] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        io.circe.parser.parse(elem) match {
          case Right(json)=>push(out,json)
          case Left(err)=>
            logger.error(s"Could not parse incoming json: $err")
            fail(out, new RuntimeException("Could not parse incoming json"))
        }
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}
