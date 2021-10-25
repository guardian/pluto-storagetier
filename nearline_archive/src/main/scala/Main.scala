import Main.logger
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.storagetier.framework.{ConnectionFactoryProvider, ConnectionFactoryProviderReal, DatabaseProvider, MessageProcessingFramework, ProcessorConfiguration}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecordDAO
import com.gu.multimedia.storagetier.models.nearline_archive.FailureRecordDAO
import de.geekonaut.slickmdc.MdcExecutionContext
import matrixstore.MatrixStoreEnvironmentConfigProvider
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit lazy val executionContext = new MdcExecutionContext(
    ExecutionContext.fromExecutor(
      Executors.newWorkStealingPool(10)
    )
  )
  private val OUTPUT_EXCHANGE_NAME = "storagetier-nearline-archive"
  //this will raise an exception if it fails, so do it as the app loads so we know straight away.
  //for this reason, don't declare this as `lazy`; if it's gonna crash, get it over with.
  private lazy val db = DatabaseProvider.get()
  private implicit val rmqConnectionFactoryProvider:ConnectionFactoryProvider =  ConnectionFactoryProviderReal

  private implicit lazy val actorSystem:ActorSystem = ActorSystem("storagetier-nearlinearchive", defaultExecutionContext=Some
  (executionContext))
  private implicit lazy val mat:Materializer = Materializer(actorSystem)

  private lazy val matrixStoreConfig = new MatrixStoreEnvironmentConfigProvider().get() match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect matrix-store config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  def main(args:Array[String]):Unit = {
    implicit lazy val nearlineRecordDAO = new NearlineRecordDAO(db)
    implicit lazy val failureRecordDAO = new FailureRecordDAO(db)
    implicit lazy val matrixStore = new MXSConnectionBuilder(
      hosts = matrixStoreConfig.hosts,
      accessKeyId = matrixStoreConfig.accessKeyId,
      accessKeySecret = matrixStoreConfig.accessKeySecret
    )

    val config = Seq(
      ProcessorConfiguration(
        "assetsweeper",
        "assetsweeper.asset_folder_importer.file.#",
        "storagetier.nearlinearchive.newfile",
        new AssetSweeperMessageProcessor()
      )
    )

    MessageProcessingFramework(
      "storagetier-nearline-archive",
      OUTPUT_EXCHANGE_NAME,
      "pluto.storagetier.nearline-archive",
      "storagetier-nearline-archive-retry",
      "storagetier-nearline-archive-fail",
      "storagetier-nearline-archive-dlq",
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
          nearlineRecordDAO.initialiseSchema,
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
