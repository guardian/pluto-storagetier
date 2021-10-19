package streamcomponents

import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.CopyReport
import org.slf4j.LoggerFactory

/**
  * diverts failed validations to a "no" channel
  * @param treatNoneAsSuccess if validation is None, then treat it as a "yes" if set, otherwise it is a "No"
  * @tparam T extraData type for CopyReport
  */
class ValidationSwitch[T](treatNoneAsSuccess:Boolean, treatPreExistingAsSuccess:Boolean=true) extends GraphStage[UniformFanOutShape[CopyReport[T], CopyReport[T]]] {
  private final val in:Inlet[CopyReport[T]] = Inlet.create("ValidationSwitch.in")
  private final val yes:Outlet[CopyReport[T]] = Outlet.create("ValidationSwitch.yes")
  private final val no:Outlet[CopyReport[T]] = Outlet.create("ValidationSwitch.no")

  override def shape: UniformFanOutShape[CopyReport[T], CopyReport[T]] = UniformFanOutShape(in,yes,no)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        if(treatPreExistingAsSuccess && elem.preExisting){
          push(yes,elem)
        } else {
          elem.validationPassed match {
            case Some(true) => push(yes, elem)
            case Some(false) => push(no, elem)
            case None => if (treatNoneAsSuccess) push(yes, elem) else push(no, elem)
          }
        }
      }
    })

    val outHandler = new AbstractOutHandler {
      override def onPull(): Unit = if(!hasBeenPulled(in)) pull(in)
    }

    setHandler(yes, outHandler)
    setHandler(no, outHandler)
  }
}