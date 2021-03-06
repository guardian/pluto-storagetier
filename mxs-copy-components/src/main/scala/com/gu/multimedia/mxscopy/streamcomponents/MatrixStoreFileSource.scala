package com.gu.multimedia.mxscopy.streamcomponents

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, GraphDSL, Source}

import java.io.InputStream
import java.nio.ByteBuffer
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import com.om.mxs.client.japi.{AccessOption, MatrixStore, MxsObject, SeekableByteChannel, UserInfo, Vault}
import com.gu.multimedia.mxscopy.helpers.VaultExtensions
import org.slf4j.LoggerFactory
import com.gu.multimedia.mxscopy.helpers.VaultExtensions._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class MatrixStoreFileSource(vault:Vault,
                            sourceId:String,
                            bufferSize:Int=2*1024*1024,
                            timeout:FiniteDuration=5.seconds,
                            maxTimeout:FiniteDuration=60.seconds
                           )
                           (implicit actorSystem:ActorSystem) extends GraphStage[SourceShape[ByteString]]{
  private final val out:Outlet[ByteString] = Outlet.create("MatrixStoreFileSource.out")

  override def shape: SourceShape[ByteString] = SourceShape.of(out)

  protected def tryToGetStream(vault:Vault)(implicit ec:ExecutionContext) = for {
    mxsFile <- vault.getObjectWithTimeout(sourceId, timeout)
    stream <- if(mxsFile.exists()) Future { mxsFile.newInputStream() } else Future.failed(new RuntimeException(s"File ${mxsFile.getId} does not exist on ${mxsFile.getVault.getId}"))
  } yield (mxsFile, stream)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)
    private var stream:InputStream = _
    private var mxsFile:MxsObject = _

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        val bytes = new Array[Byte](bufferSize)
        val bytesRead = stream.read(bytes,0,bufferSize)

        if(bytesRead == -1){
          logger.debug(s"MXS file read on ${mxsFile.getId} completed")
          complete(out)
        } else {
          logger.debug(s"Pushing $bytesRead bytes into the stream...")

          //ensure that final chunk is written with correct size
          val finalBytes = if(bytesRead==bufferSize){
            bytes
          } else {
            val nb = new Array[Byte](bytesRead)
            for(i<- 0 until bytesRead) nb.update(i, bytes(i))
            nb
          }
          push(out,ByteString(finalBytes))
        }
      }
    })

    override def preStart(): Unit = {
      implicit val ec:ExecutionContext = actorSystem.dispatcher

      Try { Await.result(tryToGetStream(vault), maxTimeout) } match {
        case Success((newFile,newStream))=>
          mxsFile = newFile
          stream = newStream
        case Failure(exception)=>
          logger.error(s"Unable to start streaming object $sourceId from ${vault.getId}: ${exception.getMessage}", exception)
          failStage(exception)
      }
    }

    override def postStop(): Unit = {
      logger.debug("post-stop, terminating stream")
      try {
        if(stream!=null){
          logger.debug("closing stream")
          stream.close()
        } else {
          logger.debug("no stream to close")
        }
      } catch {
        case err:Throwable=>
          logger.error("could not close stream: ", err)
      }
    }
  }
}

object MatrixStoreFileSource {
  /**
   * convenience builder that generates a Source[ByteString, NotUsed] for the given vault and object ID
   * @param sourceVault vault to read
   * @param sourceOID object ID to read
   * @param sys implicitly provided actor system
   * @param mat implicitly provided materializer
   * @return a Source, suitable for simplified akka chaining
   */
  def apply(sourceVault:Vault, sourceOID:String)(implicit sys:ActorSystem, mat:Materializer) = Source.fromGraph(
    GraphDSL.create() { implicit builder=>
      val s = builder.add(new MatrixStoreFileSource(sourceVault, sourceOID))
      SourceShape(s.out)
    }
  )
}