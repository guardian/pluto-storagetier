package streamcomponents

import java.io.{File, FileInputStream}
import java.lang.reflect.Method
import java.nio.{BufferUnderflowException, MappedByteBuffer}
import java.nio.channels.FileChannel

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic}
import akka.util.ByteString
import org.slf4j.LoggerFactory
import sun.nio.ch.FileChannelImpl

import scala.util.{Failure, Success, Try}

class MMappedFileSource(file:File, pageSize:Int=2*1024*1024) extends GraphStage[SourceShape[ByteString]] {
  private final val out:Outlet[ByteString] = Outlet.create("MMappedFileSource.out")

  //map in 1Gb chunks
  private val mapSize:Long = 1073741824

  override def shape: SourceShape[ByteString] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)
    logger.info(s"Starting up")
    private var stream:FileInputStream = _
    private var ctr:Long = 0
    private var mapChunks:IndexedSeq[MappedByteBuffer] = IndexedSeq()

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {

        if (ctr >= file.length()) {
          logger.info(s"Finished streaming ${file.getAbsolutePath}")
          complete(out)
        } else {
          Try {
            val chunkIndex = Math.floor(ctr.toDouble / mapSize.toDouble).toInt
            val chunkPtr = (ctr - (chunkIndex * mapSize)).toInt
            logger.debug(s"chunkIndex is $chunkIndex, chunkPtr is $chunkPtr with ctr $ctr and mapSize $mapSize")

            try {
              val bytes: Array[Byte] = new Array[Byte](pageSize.toInt)
              mapChunks(chunkIndex).get(bytes, 0, pageSize)
              bytes
            } catch {
              case ex:BufferUnderflowException=>
                val lastPageSize = file.length()-ctr
                val bytes: Array[Byte] = new Array[Byte](lastPageSize.toInt)
                mapChunks(chunkIndex).get(bytes, 0, lastPageSize.toInt)
                bytes
            }

          } match {
            case Success(bytes) =>
              logger.debug(s"Pushing $pageSize bytes from $ctr...")
              ctr += pageSize
              push(out, ByteString(bytes))
            case Failure(err) =>
              logger.error(s"Could not read bytes from mmap: ", err)
              failStage(err)
          }
        }
      }
    })

    override def preStart(): Unit = {
      logger.info(s"Opening ${file.getAbsolutePath}, chunk size is $pageSize...")
      try {
        stream = new FileInputStream(file)

        val mapChunkCount = Math.ceil(file.length().toDouble / mapSize.toDouble).toInt
        val lastChunkSize = file.length() - ((mapChunkCount-1) * mapSize)
        logger.debug(s"mapChunkCount is $mapChunkCount, lastChunkSize is $lastChunkSize bytes")
        mapChunks = for(i <- 0 until mapChunkCount) yield stream.getChannel.map(FileChannel.MapMode.READ_ONLY, i*mapSize, if(i!=mapChunkCount-1) mapSize else lastChunkSize)
      } catch {
        case err:Throwable=>
          logger.error(s"Could not open ${file.getAbsolutePath}", err)
          failStage(err)
      }
    }

    override def postStop(): Unit = {
      stream.close()
    }
  }
}
