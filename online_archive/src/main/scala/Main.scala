import com.gu.multimedia.storagetier.framework._
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  //this will raise an exception if it fails, so do it as the app loads so we know straight away.
  //for this reason, don't declare this as `lazy`; if it's gonna crash, get it over with.
  private val db = DatabaseProvider.get()
  private implicit val rmqConnectionFactoryProvider =  ConnectionFactoryProviderReal

  val config = Seq(
    ProcessorConfiguration(
      "test-input-exchange",
      "#",
      new FakeMessageProcessor(db)
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
          case Failure(err) =>
            logger.error(s"framework run failed: ${err.getMessage}", err)
            sys.exit(1)
        })
    }
  }
}
