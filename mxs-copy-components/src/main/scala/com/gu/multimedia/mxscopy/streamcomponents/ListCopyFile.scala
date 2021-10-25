package com.gu.multimedia.mxscopy.streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Materializer, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, AsyncCallback, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{UserInfo, Vault}
import com.gu.multimedia.mxscopy.helpers.Copier
import com.gu.multimedia.mxscopy.models.{CopyReport, IncomingListEntry}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * perform a single file copy in a bulk file copy operation.  This will spin up an entire substream to perform
  * the file copy.
  * @param vault
  * @param chunkSize
  * @param checksumType
  * @param mat
  */
class ListCopyFile[T](userInfo:UserInfo, vault:Vault,chunkSize:Int, checksumType:String, implicit val mat:Materializer)
  extends GraphStage[FlowShape[IncomingListEntry,CopyReport[T]]] {
  private final val in:Inlet[IncomingListEntry] = Inlet.create("ListCopyFile.in")
  private final val out:Outlet[CopyReport[T]] = Outlet.create("ListCopyFile.out")

  override def shape: FlowShape[IncomingListEntry, CopyReport[T]] = FlowShape.of(in,out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        logger.debug(s"listCopyFile: onPush")
        val entry = grab(in)

        logger.info(s"Starting copy of ${entry.filepath}")
        val completedCb = createAsyncCallback[CopyReport[T]](report=>push(out, report))
        val failedCb = createAsyncCallback[Throwable](err=>failStage(err))

        Copier.copyFromLocal(userInfo, vault, Some(entry.filePath), entry.filepath, chunkSize, checksumType).onComplete({
          case Success(Right( (oid,maybeChecksum) ))=>
            logger.info(s"Copied ${entry.filepath} to $oid")
            completedCb.invoke(CopyReport[T](entry.filePath, oid, maybeChecksum, entry.size, preExisting = false, validationPassed = None))
          case Success(Left(copyProblem))=>
            logger.warn(s"Could not copy file: $copyProblem")
            completedCb.invoke(CopyReport[T](entry.filePath, copyProblem.filepath.oid, None, entry.size, preExisting = true, validationPassed = None))
          case Failure(err)=>
            logger.info(s"Failed copying ${entry.filepath}", err)
            failedCb.invoke(err)
        })
      }
    })

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        logger.debug("listCopyFile: onPull")
        pull(in)
      }
    })
  }
}
