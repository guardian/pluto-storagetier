package streamcomponents

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic, GraphStageWithMaterializedValue}
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, SearchTerm, UserInfo, Vault}
import models.ObjectMatrixEntry
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/**
  * Akka source that queries the given search term on the given ObjectMatrix cluster
  * @param userInfo object matrix connection structure
  * @param searchTerm term to search for
  * @param atOnce number of results to pull at once
  */
class OMSearchSource (userInfo:UserInfo, searchTerm:Option[SearchTerm], searchAttribute:Option[Attribute], atOnce:Int=10) extends GraphStageWithMaterializedValue[SourceShape[ObjectMatrixEntry],Future[Int]]{
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("OMSearchSource.out")

  override def shape: SourceShape[ObjectMatrixEntry] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
    val promise = Promise[Int]()

    val logic = new GraphStageLogic(shape) {
      private val logger = LoggerFactory.getLogger(getClass)
      var vault: Option[Vault] = None
      var iterator: Option[Iterator[String]] = None
      var ctr:Int=0

      setHandler(out, new AbstractOutHandler {
        override def onPull(): Unit = {
          iterator match {
            case None =>
              logger.error(s"Can't iterate before connection was established")
              failStage(new RuntimeException)
            case Some(iter) =>
              if (iter.hasNext) {
                val oid = iter.next()
                val elem = ObjectMatrixEntry(oid, None,None)
                push(out, elem)
                ctr+=1
              } else {
                logger.info(s"Completed iterating results")
                complete(out)
              }
          }
        }

      })

      override def preStart(): Unit = {
        //establish connection to OM
        try {
          logger.info(s"Establishing connection to ${userInfo.getVault} on ${userInfo.getAddresses} as ${userInfo.getUser}")
          vault = Some(MatrixStore.openVault(userInfo))
          iterator = searchTerm match {
            case Some(actualSearchTerm)=>Some(vault.get.searchObjectsIterator(actualSearchTerm, atOnce).asScala)
            case None=>
              searchAttribute match {
                case Some(actualSearchAttribute)=>Some(vault.get.searchObjectsIterator(actualSearchAttribute,atOnce).asScala)
                case None=>Some(vault.get.searchObjectsIterator(new Attribute(Constants.CONTENT, "*"), atOnce).asScala)
              }
          }

          logger.info(s"Connection established")
        } catch {
          case ex: Throwable =>
            logger.error(s"Could not establish connection: ", ex)
            failStage(ex)
        }
      }

      override def postStop(): Unit = {
        if(vault.isDefined) vault.get.dispose()
        promise.success(ctr)
      }
    }

    (logic, promise.future)
  }
}
