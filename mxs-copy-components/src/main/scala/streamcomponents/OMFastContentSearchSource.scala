package streamcomponents

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{Attribute, Constants, MatrixStore, SearchTerm, UserInfo, Vault}
import models.{MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class OMFastContentSearchSource(userInfo:UserInfo, contentSearchString:String, keywords:Array[String], atOnce:Int=100) extends GraphStage[SourceShape[ObjectMatrixEntry]] {
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("OMFastSearchSource.out")

  override def shape: SourceShape[ObjectMatrixEntry] = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger:org.slf4j.Logger = LoggerFactory.getLogger(getClass)

    def parseOutResults(resultString:String) = {
      logger.debug(s"parseOutResults: got $resultString")
      val parts = resultString.split("\n")

      val kvs = parts.tail
        .map(_.split("="))
        .foldLeft(Map[String,String]()) ((acc,elem)=>acc ++ Map(elem.head -> elem.tail.mkString("=")))
      logger.debug(s"got $kvs")
      val mxsMeta = MxsMetadata(kvs,Map(),Map(),Map())

      logger.debug(s"got $mxsMeta")
      ObjectMatrixEntry(parts.head,attributes = Some(mxsMeta), fileAttribues = None)
    }

    var vault: Option[Vault] = None
    var iterator: Option[Iterator[String]] = None

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = {
        iterator match {
          case None =>
            logger.error(s"Can't iterate before connection was established")
            failStage(new RuntimeException)
          case Some(iter) =>
            if (iter.hasNext) {
              val resultString = iter.next()
              val elem = parseOutResults(resultString)
              logger.debug(s"Got element $elem")
              push(out, elem)
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
        logger.debug("OMFastSearchSource starting up")
        logger.info(s"Establishing connection to ${userInfo.getVault} on ${userInfo.getAddresses} as ${userInfo.getUser}")
        vault = Some(MatrixStore.openVault(userInfo))

        val searchTermString = if(keywords.nonEmpty) {
          contentSearchString + "\n" + "keywords: " + keywords.mkString(",")
        } else {
          contentSearchString
        }
        logger.debug(s"search string is '$searchTermString'")
        iterator = vault.map(_.searchObjectsIterator(SearchTerm.createSimpleTerm(Constants.CONTENT, searchTermString), atOnce).asScala)
        logger.info("Connection established")

      } catch {
        case ex: Throwable =>
          logger.error(s"Could not establish connection: ", ex)
          failStage(ex)
      }
    }

    override def postStop(): Unit = {
      logger.info("Search stream stopped")
      vault.map(_.dispose())
    }
  }
}

