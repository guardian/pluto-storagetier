package streamcomponents

import java.io.File

import akka.stream.{Attributes, Inlet, Outlet, UniformFanOutShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import models.IncomingListEntry
import org.slf4j.LoggerFactory


/**
  * filters out non-existing and directory files.
  */
class FilesFilter(failOnNonexisting:Boolean=false)  extends GraphStage[UniformFanOutShape[IncomingListEntry,IncomingListEntry]] {
  private val in:Inlet[IncomingListEntry] = Inlet.create("FilesFilter.in")
  private val yes:Outlet[IncomingListEntry] = Outlet.create("FilesFilter.yes")
  private val no:Outlet[IncomingListEntry] = Outlet.create("FilesFilter.no")

  override def shape: UniformFanOutShape[IncomingListEntry, IncomingListEntry] = UniformFanOutShape(in, yes, no)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)
        logger.debug("filesFilter: onPush")

        val file = new File(elem.filepath)

        if(file.isDirectory){
          logger.info(s"${elem.filepath} is a directory, leaving it")
          push(no, elem)
        } else if(!file.exists()){
          if(failOnNonexisting) {
            logger.warn(s"${elem.filepath} does not exist, aborting")
            failStage(new RuntimeException(s"${elem.filepath} does not exist"))
          } else {
            logger.warn(s"${elem.filepath} does not exist, leaving it")
            push(no, elem)
          }
        } else {
          logger.info(s"${elem.filepath} exists, passing")
          while(!isAvailable(yes)) Thread.sleep(50)
          push(yes, elem)
        }
      }
    })

    setHandler(yes, new AbstractOutHandler {
      override def onPull(): Unit = {
        logger.debug("filesFilter YES: onPull")
        if(!hasBeenPulled(in)) pull(in)
      }
    })

    setHandler(no, new AbstractOutHandler {
      override def onPull(): Unit = {
        logger.debug("filesFilter NO: onPull")
        if(!hasBeenPulled(in)) pull(in)
      }
    })
  }
}
