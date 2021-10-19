package streamcomponents

import java.time.Instant

import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import akka.stream.{Attributes, Inlet, SinkShape}
import models.{CopyReport, FinalReport}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}

class ProgressMeterAndReport[T](maybeTotalFiles:Option[Long]=None, maybeTotalBytes:Option[Long]=None) extends GraphStageWithMaterializedValue[SinkShape[CopyReport[T]],Future[FinalReport]] {
  private final val in: Inlet[CopyReport[T]] = Inlet.create("ProgressMeterAndReport.in")

  override def shape: SinkShape[CopyReport[T]] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[FinalReport]) = {
    val completionPromise = Promise[FinalReport]()

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)
      private var startTime: Long = _
      private var bytesCopied: Long = _
      private var filesCopied: Long = _
      private var preExisting: Long = _

      setHandler(in, new AbstractInHandler {
        override def onPush(): Unit = {
          val elem = grab(in)

          bytesCopied += elem.size
          filesCopied += 1
          if(elem.preExisting) preExisting+=1

          val gbCopied = bytesCopied.toDouble / 1073741824
          val elapsedTime = Instant.now().toEpochMilli - startTime
          val mbPerSec = gbCopied.toDouble * 1024000.0 / elapsedTime.toDouble
          logger.info(f"Running total: Copied $filesCopied files ($preExisting were already there) totalling $gbCopied%.3f Gb in $elapsedTime msec, at a rate of $mbPerSec%.2f mb/s")
          if(maybeTotalBytes.isDefined){
            val pct = ((gbCopied*1073741824) / maybeTotalBytes.get ) * 100
            logger.info(f"$pct%.1f%% complete by bytes")
          }
          if(maybeTotalFiles.isDefined){
            val pct = (filesCopied.toDouble / maybeTotalFiles.get.toDouble) * 100
            logger.info(f"$pct%.1f%% complete by file count")
          }
          pull(in)
        }
      })

      override def preStart(): Unit = {
        startTime = Instant.now().toEpochMilli
        pull(in)
      }

      override def postStop(): Unit = {
        val gbCopied = bytesCopied.toDouble / 1073741824
        val elapsedTime = Instant.now().toEpochMilli - startTime
        val mbPerSec = gbCopied.toDouble * 1024000.0 / elapsedTime.toDouble
        completionPromise.success(FinalReport(bytesCopied.toDouble / 1073741824, filesCopied, elapsedTime, mbPerSec))
      }
    }
    (logic, completionPromise.future)
  }
}