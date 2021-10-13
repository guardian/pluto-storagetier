import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Keep, Sink}
import com.gu.multimedia.storagetier.vidispine.{VidispineCommunicator, VidispineConfig}
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit lazy val actorSystem = ActorSystem("vidispine-test")
  implicit lazy val mat = Materializer.matFromSystem
  implicit lazy val ec:ExecutionContext = actorSystem.dispatcher

  def getVidispineCommunicator = VidispineConfig.fromEnvironment match {
    case Left(err)=>
      logger.error(err)
      sys.exit(1)
    case Right(vsConfig)=>
      new VidispineCommunicator(vsConfig)
  }

  def testGetThumbnail(vsComm:VidispineCommunicator, itemId:String) = vsComm.akkaStreamFirstThumbnail(itemId, None).flatMap({
    case None=>
      logger.error(s"No thumbnail present on $itemId")
      Future( () )
    case Some(source)=>
      source.dataBytes
        .toMat(FileIO.toPath(Paths.get("test.jpg")))(Keep.right)
        .run()
        .map(ioResult=>ioResult.status match {
          case Success(_)=>
            logger.info(s"Written ${ioResult.count} bytes to test.jpg.")
          case Failure(err)=>
            logger.error(s"Could not write to test.jpg: ${err.getMessage}")
        })
  })

  def testGetMeta(vsComm:VidispineCommunicator, itemId:String) = vsComm.akkaStreamXMLMetadataDocument(itemId).flatMap({
    case None=>
      logger.error(s"No metadata present on $itemId")
      Future( () )
    case Some(source)=>
      val path = Paths.get(s"$itemId.xml")
      source.dataBytes
        .toMat(FileIO.toPath(path))(Keep.right)
        .run()
        .map(ioResult=>ioResult.status match {
          case Success(_)=>
            logger.info(s"Written ${ioResult.count} bytes to $path.")
          case Failure(err)=>
            logger.error(s"Could not write to $path: ${err.getMessage}")
        })
  })

  def main(args:Array[String]) = {
    val vsComm = getVidispineCommunicator

    args.headOption match {
      case None=>
        logger.error("You must specify an item ID to query as a program argument")
        actorSystem.terminate()
      case Some(itemId)=>
        logger.info(s"Getting first thumbnail for $itemId...")
        Future.sequence(Seq(
          testGetThumbnail(vsComm, itemId),
          testGetMeta(vsComm, itemId)
        ))
          .map(_=>{
            logger.info("all done, terminating actor system")
            actorSystem.terminate()
          })
          .recoverWith({
            case err:Throwable=>
              logger.error("vidispine communicator failed: ", err)
              actorSystem.terminate()
          })
    }
    ()
  }
}
