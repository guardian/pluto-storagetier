import akka.actor.ActorSystem
import akka.http.scaladsl.{ConnectionContext, Http}
import akka.stream.Materializer
import archivehunter.{ArchiveHunterCommunicator, ArchiveHunterEnvironmentConfigProvider}
import com.gu.multimedia.storagetier.framework._
import com.gu.multimedia.storagetier.models.online_archive.{ArchivedRecordDAO, FailureRecordDAO, IgnoredRecordDAO}
import com.gu.multimedia.storagetier.vidispine.{VidispineCommunicator, VidispineConfig}
import de.geekonaut.slickmdc.MdcExecutionContext
import org.slf4j.LoggerFactory
import com.gu.multimedia.storagetier.plutocore.PlutoCoreEnvironmentConfigProvider
import plutodeliverables.PlutoDeliverablesConfig
import sun.misc.{Signal, SignalHandler}
import utils.TrustStoreHelper

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit lazy val executionContext = new MdcExecutionContext(
    ExecutionContext.fromExecutor(
      Executors.newWorkStealingPool(10)
    )
  )
  private val OUTPUT_EXCHANGE_NAME = "storagetier-online-archive"
  //this will raise an exception if it fails, so do it as the app loads so we know straight away.
  //for this reason, don't declare this as `lazy`; if it's gonna crash, get it over with.
  private lazy val db = DatabaseProvider.get()
  private implicit val rmqConnectionFactoryProvider:ConnectionFactoryProvider =  ConnectionFactoryProviderReal

  private implicit lazy val actorSystem:ActorSystem = ActorSystem("storagetier-onlinearchive", defaultExecutionContext=Some(executionContext))
  private implicit lazy val mat:Materializer = Materializer(actorSystem)
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

  private lazy val vidispineConfig = VidispineConfig.fromEnvironment match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect Vidispine config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  sys.env.get("LOCAL_TRUST_STORE") match {
    case Some(localTrustStore)=>
      logger.info(s"Adding local trust store at $localTrustStore")
      TrustStoreHelper.setupTS(Seq(localTrustStore)) match {
        case Success(context)=>
          Http().setDefaultClientHttpsContext(ConnectionContext.https(context))
        case Failure(err)=>
          logger.error("Could not set up local trust store: ", err)
          sys.exit(1)
      }
    case None=>
      logger.info(s"No separate local trust store is set up.")
  }

  def main(args:Array[String]):Unit = {
    implicit lazy val archivedRecordDAO = new ArchivedRecordDAO(db)
    implicit lazy val failureRecordDAO = new FailureRecordDAO(db)
    implicit lazy val ignoredRecordDAO = new IgnoredRecordDAO(db)
    implicit lazy val archiveHunterCommunicator = new ArchiveHunterCommunicator(archiveHunterConfig)
    implicit lazy val vidispineCommunicator = new VidispineCommunicator(vidispineConfig)

    implicit lazy val uploader = FileUploader.createFromEnvVars("ARCHIVE_MEDIA_BUCKET") match {
      case Left(err)=>
        logger.error(s"Could not initialise FileUploader: $err")
        Await.ready(actorSystem.terminate(), 30.seconds)
        sys.exit(1)
      case Right(u)=>u
    }

    lazy val proxyUploader = FileUploader.createFromEnvVars("ARCHIVE_PROXY_BUCKET") match {
      case Left(err)=>
        logger.error(s"Could not initialise ProxyFileUploader: $err")
        Await.ready(actorSystem.terminate(), 30.seconds)
        sys.exit(1)
      case Right(u)=>u
    }

    lazy implicit val vidispineFunctions = new VidispineFunctions(uploader, proxyUploader)
    val deliverablesConfig = PlutoDeliverablesConfig()

    val config = Seq(
      ProcessorConfiguration(
        "assetsweeper",
        Seq("assetsweeper.asset_folder_importer.file.new","assetsweeper.asset_folder_importer.file.update", "assetsweeper.replay.file"),
        Seq("storagetier.onlinearchive.newfile","storagetier.onlinearchive.newfile","storagetier.onlinearchive.replay"),
        new AssetSweeperMessageProcessor(plutoConfig)
      ),
      ProcessorConfiguration(
        "vidispine-events",
        Seq("vidispine.job.raw_import.stop", "vidispine.job.essence_version.stop", "vidispine.item.shape.modify", "vidispine.item.metadata.modify", "vidispine.itemneedsarchive.nearline", "vidispine.itemneedsarchive.online"),
        Seq("storagetier.onlinearchive.newfile", "storagetier.onlinearchive.newfile", "storagetier.onlinearchive.vidispineupdate", "storagetier.onlinearchive.vidispineupdate", "storagetier.onlinearchive.newfile.nearline", "storagetier.onlinearchive.newfile.online"),
        new VidispineMessageProcessor(plutoConfig, deliverablesConfig)
      ),
      ProcessorConfiguration(
        OUTPUT_EXCHANGE_NAME,
        Seq("storagetier.onlinearchive.newfile.success","storagetier.onlinearchive.request.*","storagetier.onlinearchive.replay.success","storagetier.onlinearchive.newfile.nearline.success","storagetier.onlinearchive.newfile.online.success"),
        Seq("storagetier.onlinearchive.mediaingest","storagetier.onlinearchive.requested","storagetier.onlinearchive.replayed","storagetier.onlinearchive.mediaingest.nearline","storagetier.onlinearchive.mediaingest.online"),
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
