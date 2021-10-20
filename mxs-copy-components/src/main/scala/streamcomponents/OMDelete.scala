package streamcomponents

import akka.stream.scaladsl.{Flow, GraphDSL, Sink}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet, SinkShape}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.{MatrixStore, UserInfo, Vault}
import models.ObjectMatrixEntry
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Akka streams Flow that will delete the incoming ObjectMatrixEntry from the provided Vault.  Errors will terminate the stream.
 * Emits the same [[ObjectMatrixEntry]] element that came in, but it should have been deleted from disk by that point.
 * The stream element makes its own connection to the ObjectMatrix vault from the provided UserInfo object at stream
 * start-up and terminates it at teardown.
 * You don't need to construct this directly, see the methods on the companion object to initialise as a Sink, or a Flow
 *
 * @param vaultInfo ObjectMatrix SDK UserInfo object describing the vault to connect to
 * @param reallyDelete if false, then a message will be output but nothing will actually be deleted. If true then content
 *                     is removed.
 * @param failOnError if true, then any error is fatal.  If false, then errors will be logged as warnings but the stream
 *                    allowed to continue. Defaults to false.
 */
class OMDelete(vaultInfo:UserInfo, reallyDelete:Boolean, failOnError:Boolean=false) extends GraphStage[FlowShape[ObjectMatrixEntry, ObjectMatrixEntry]] {
  private final val in:Inlet[ObjectMatrixEntry] = Inlet.create("OMDelete.in")
  private final val out:Outlet[ObjectMatrixEntry] = Outlet.create("OMDelete.out")
  private val logger = LoggerFactory.getLogger(getClass)

  override def shape: FlowShape[ObjectMatrixEntry, ObjectMatrixEntry] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var vault:Vault = _

    override def preStart(): Unit = {
      try {
        vault = MatrixStore.openVault(vaultInfo)
      } catch {
        case err:Throwable=>
          logger.error("Could not connect to MatrixStore: ", err)
          failStage(err)
          throw err
      }
    }

    override def postStop(): Unit = {
      if(vault!=null) vault.dispose()
    }

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        if(reallyDelete) {
          logger.debug(s"Deleting ${elem.oid} (${elem.maybeGetFilename()})...")
          OMDelete.doDelete(vault, elem) match {
            case Success(_)=>
              logger.info(s"Successfully deleted ${elem.oid} (${elem.maybeGetFilename()})")
              push(out, elem)
            case Failure(err)=>
              logger.error(s"Could not delete ${elem.oid}: ${err.getMessage}")
              if(failOnError) {
                failStage(err)
              } else {
                pull(in)
              }
          }
        } else {
          logger.warn(s"If reallyDelete were true then I would attempt to delete ${elem.oid} (${elem.maybeGetFilename()})")
          push(out, elem)
        }
      }
    })
  }
}

object OMDelete {
  /**
   * create an instance of [[OMDelete]] as a Sink.  This received ObjectMatrixEntry elements and does not materialize a value.
   * @param vaultInfo ObjectMatrix SDK UserInfo object describing the vault to connect to
   * @param reallyDelete if false, then a message will be output but nothing will actually be deleted. If true then content
   *                     is removed.
   * @param failOnError if true, then any error is fatal.  If false, then errors will be logged as warnings but the stream
   *                    allowed to continue. Defaults to false.
   * @return a ready-to-run Sink instance, suitable for Source.to(sink) syntax
   */
  def sink(vaultInfo:UserInfo, reallyDelete:Boolean, failOnError:Boolean=false) = Sink.fromGraph(GraphDSL.create() { implicit builder=>
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val deleter = builder.add(new OMDelete(vaultInfo, reallyDelete, failOnError))
    val s = builder.add(Sink.ignore)

    deleter ~> s
    SinkShape(deleter.in)
  })

  /**
   * create an instance of [[OMDelete]] as a Flow.  This receives ObjectMatrixEntry elements and emits them once deleted.
   * @param vaultInfo ObjectMatrix SDK UserInfo object describing the vault to connect to
   * @param reallyDelete if false, then a message will be output but nothing will actually be deleted. If true then content
   *                     is removed.
   * @param failOnError if true, then any error is fatal.  If false, then errors will be logged as warnings but the stream
   *                    allowed to continue. Defaults to false.
   * @return a ready-to-run Flow instance, suitable for Source.via(flow) syntax
   */
  def flow(vaultInfo:UserInfo, reallyDelete:Boolean, failOnError:Boolean=false) = Flow.fromGraph(GraphDSL.create() { implicit builder=>
    val deleter = builder.add(new OMDelete(vaultInfo, reallyDelete, failOnError))

    FlowShape(deleter.in, deleter.out)
  })

  /**
   * static deletion method.  Simply request a deletion and a Success or Failure is returned
   * @param vault vault instance to delete from. Get this from MatrixStore.openVault()
   * @param elem [[ObjectMatrixEntry]] to delete
   * @return a Success with no value if the deletion was OK, otherwise a Failure with the given exception.
   */
  def doDelete(vault:Vault, elem:ObjectMatrixEntry) = Try {
    val omFile = vault.getObject(elem.oid)
    omFile.delete()
  }

}