import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.framework.{ConnectionFactoryProvider, ConnectionFactoryProviderReal, MessageProcessingFramework, ProcessorConfiguration}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreEnvironmentConfigProvider}
import com.gu.multimedia.storagetier.vidispine.{VidispineCommunicator, VidispineConfig}
import de.geekonaut.slickmdc.MdcExecutionContext
import matrixstore.MatrixStoreEnvironmentConfigProvider
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit lazy val executionContext = new MdcExecutionContext(
    ExecutionContext.fromExecutor(
      Executors.newWorkStealingPool(10)
    )
  )
  private val OUTPUT_EXCHANGE_NAME = "storagetier-project-restorer"

  private implicit val rmqConnectionFactoryProvider:ConnectionFactoryProvider =  ConnectionFactoryProviderReal
  private val connectionIdleTime = sys.env.getOrElse("CONNECTION_MAX_IDLE", "750").toInt
  private implicit lazy val actorSystem:ActorSystem = ActorSystem("storagetier-projectrestorer", defaultExecutionContext=Some
  (executionContext))
  private implicit lazy val mat:Materializer = Materializer(actorSystem)

  private lazy val plutoConfig = new PlutoCoreEnvironmentConfigProvider().get() match {
    case Left(err) =>
      logger.error(s"Could not initialise due to incorrect pluto-core config: $err")
      sys.exit(1)
    case Right(config) => config
  }

  private implicit lazy val matrixStoreConfig = new MatrixStoreEnvironmentConfigProvider().get() match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect matrix-store config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  private lazy val vidispineConfig = VidispineConfig.fromEnvironment match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect Vidispine config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  private lazy val retryLimit = sys.env.get("RETRY_LIMIT").map(_.toInt).getOrElse(200)

  def main(args:Array[String]):Unit = {

    implicit val matrixStore = new MXSConnectionBuilderImpl(
      hosts = matrixStoreConfig.hosts,
      accessKeyId = matrixStoreConfig.accessKeyId,
      accessKeySecret = matrixStoreConfig.accessKeySecret,
      clusterId = matrixStoreConfig.clusterId,
      maxIdleSeconds = connectionIdleTime
    )
    implicit lazy val vidispineCommunicator = new VidispineCommunicator(vidispineConfig)

    val assetFolderLookup = new AssetFolderLookup(plutoConfig)

    val config = Seq(
      ProcessorConfiguration(
        "pluto-core",
        "core.project.#",
        "storagetier.restorer",
        new PlutoCoreMessageProcessor(matrixStoreConfig, assetFolderLookup)
      )
    )

    MessageProcessingFramework(
      "storagetier-project-restorer",
      OUTPUT_EXCHANGE_NAME,
      "pluto.storagetier.project-restorer",
      "storagetier-project-restorer-retry",
      "storagetier-project-restorer-fail",
      "storagetier-project-restorer-dlq",
      config,
      maximumRetryLimit = retryLimit

    ) match {
      case Left(err) =>
        logger.error(s"Could not initiate message processing framework: $err")
      case Right(framework) =>
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
            logger.info(s"framework run failed: ${err.getMessage}", err)
            Await.ready(actorSystem.terminate(), 10.minutes)
            sys.exit(1)
        })
    }
  }
}
