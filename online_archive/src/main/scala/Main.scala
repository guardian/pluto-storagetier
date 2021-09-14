import com.gu.multimedia.storagetier.framework._
import org.slf4j.LoggerFactory
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
    val framework = new MessageProcessingFramework(
      "storagetier-online-archive",
      "storagetier-online-archive-out",
      "pluto.storagetier.online-archive",
      "storagetier-online-archive-retry",
      "storagetier-online-archive-fail",
      config
    )

    framework.run().onComplete({
      case Success(cid)=>
        logger.info(s"framework run completed, result string was $cid")
      case Failure(err)=>
        logger.error(s"framework run failed: ${err.getMessage}",err)
    })
  }
}
