import akka.actor.ActorSystem
import akka.stream.Materializer
import archivehunter.{ArchiveHunterCommunicator, ArchiveHunterEnvironmentConfigProvider}
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import org.slf4j.LoggerFactory
import plutocore.PlutoCoreEnvironmentConfigProvider
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  private val OUTPUT_EXCHANGE_NAME = "storagetier-online-archive"
  //this will raise an exception if it fails, so do it as the app loads so we know straight away.
  //for this reason, don't declare this as `lazy`; if it's gonna crash, get it over with.
  private lazy val db = DatabaseProvider.get()
  private implicit val rmqConnectionFactoryProvider =  ConnectionFactoryProviderReal

  private implicit lazy val actorSystem = ActorSystem()
  private implicit lazy val mat = Materializer(actorSystem)
  private lazy val plutoConfig = new PlutoCoreEnvironmentConfigProvider().get() match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect pluto-core config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  private lazy val archiveHunterConfig = new ArchiveHunterEnvironmentConfigProvider().get() match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect pluto-core config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  def main(args:Array[String]):Unit = {
    implicit lazy val archivedRecordDAO = new ArchivedRecordDAO(db)
    implicit lazy val failureRecordDAO = new FailureRecordDAO(db)
    implicit lazy val ignoredRecordDAO = new IgnoredRecordDAO(db)
    implicit lazy val archiveHunterCommunicator = new ArchiveHunterCommunicator(archiveHunterConfig)

    implicit lazy val uploader = FileUploader.createFromEnvVars match {
      case Left(err)=>
        logger.error(s"Could not initialise FileUploader: $err")
        Await.ready(actorSystem.terminate(), 30.seconds)
        sys.exit(1)
      case Right(u)=>u
    }

    val config = Seq(
      ProcessorConfiguration(
        "assetsweeper",
        "assetsweeper.asset_folder_importer.file.#",
        new AssetSweeperMessageProcessor(plutoConfig)
      ),
      ProcessorConfiguration(
        OUTPUT_EXCHANGE_NAME,
        "storagetier.onlinearchive.newfile.success",
        new OwnMessageProcessor()
      )
    )

    MessageProcessingFramework(
      "storagetier-online-archive",
      OUTPUT_EXCHANGE_NAME,
      "pluto.storagetier.online-archive",
      "storagetier-online-archive-retry",
      "storagetier-online-archive-fail",
      "storagetier-online-archive-dlq",
      config
    ) match {
      case Left(err) =>
        logger.error(s"Could not initiate message processing framework: $err")
        actorSystem.terminate()
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

        //first initialise all the tables that we need, then run the framework.
        //add in more table initialises as required
        Future.sequence(Seq(
          ignoredRecordDAO.initialiseSchema,
          archivedRecordDAO.initialiseSchema,
          failureRecordDAO.initialiseSchema,
        ))
          .flatMap(_=>framework.run())
          .onComplete({
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
