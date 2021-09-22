import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecordDAO, FailureRecordDAO}
import org.slf4j.LoggerFactory
import plutocore.PlutoCoreEnvironmentConfigProvider
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  //this will raise an exception if it fails, so do it as the app loads so we know straight away.
  //for this reason, don't declare this as `lazy`; if it's gonna crash, get it over with.
  private val db = DatabaseProvider.get()
  private implicit val rmqConnectionFactoryProvider =  ConnectionFactoryProviderReal

  private implicit lazy val archivedRecordDAO = new ArchivedRecordDAO(db)
  private implicit lazy val failureRecordDAO = new FailureRecordDAO(db)
  private implicit lazy val actorSystem = ActorSystem()
  private implicit lazy val mat = Materializer(actorSystem)
  private lazy val plutoConfig = new PlutoCoreEnvironmentConfigProvider().get() match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect pluto-core config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  val config = Seq(
    ProcessorConfiguration(
      "assetsweeper",
      "assetsweeper.asset_folder_importer.file.#",
      new AssetSweeperMessageProcessor(plutoConfig)
    )
  )

  def main(args:Array[String]) = {
    MessageProcessingFramework(
      "storagetier-online-archive",
      "storagetier-online-archive-out",
      "pluto.storagetier.online-archive",
      "storagetier-online-archive-retry",
      "storagetier-online-archive-fail",
      "storagetier-online-archive-dlq",
      config
    ) match {
      case Left(err) => logger.error(s"Could not initiate message processing framework: $err")
      case Right(framework) =>
        //install a signal handler to terminate cleanly on INT (keyboard interrupt) and TERM (Kubernetes pod shutdown)
        val terminationHandler = new SignalHandler {
          override def handle(signal: Signal): Unit = {
            logger.info(s"Caught signal $signal, terminating")
            framework.terminate()
          }
        }
        Signal.handle(new Signal("INT"), terminationHandler)
        Signal.handle(new Signal("HUP"), terminationHandler)
        Signal.handle(new Signal("TERM"), terminationHandler)

        framework.run().onComplete({
          case Success(_) =>
            logger.info(s"framework run completed")
            Await.ready(actorSystem.terminate(), 10.minutes)
            sys.exit(0)
          case Failure(err) =>
            logger.error(s"framework run failed: ${err.getMessage}", err)
            Await.ready(actorSystem.terminate(), 10.minutes)
            sys.exit(1)
        })
    }
  }
}
