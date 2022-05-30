

import Main.{connectionIdleTime, db, executionContext}
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecordDAO}
import de.geekonaut.slickmdc.MdcExecutionContext
import matrixstore.MatrixStoreEnvironmentConfigProvider
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit lazy val executionContext = new MdcExecutionContext(
    ExecutionContext.fromExecutor(
      Executors.newWorkStealingPool(10)
    )
  )
  private val connectionIdleTime = sys.env.getOrElse("CONNECTION_MAX_IDLE", "750").toInt
  private implicit lazy val actorSystem:ActorSystem = ActorSystem("storagetier-projectrestorer", defaultExecutionContext=Some
  (executionContext))
  private implicit lazy val mat:Materializer = Materializer(actorSystem)

  private implicit lazy val matrixStoreConfig = new MatrixStoreEnvironmentConfigProvider().get() match {
    case Left(err)=>
      logger.error(s"Could not initialise due to incorrect matrix-store config: $err")
      sys.exit(1)
    case Right(config)=>config
  }

  def main(args:Array[String]):Unit = {

    implicit val matrixStore = new MXSConnectionBuilderImpl(
      hosts = matrixStoreConfig.hosts,
      accessKeyId = matrixStoreConfig.accessKeyId,
      accessKeySecret = matrixStoreConfig.accessKeySecret,
      clusterId = matrixStoreConfig.clusterId,
      maxIdleSeconds = connectionIdleTime
    )
  }
}
