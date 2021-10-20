package streamcomponents

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{AbstractInHandler, AbstractOutHandler, GraphStage, GraphStageLogic}
import com.om.mxs.client.japi.Vault
import helpers.MatrixStoreHelper
import models.CopyReport
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * flow stage to validate an MD5 checksum, i.e. check if the one from the copy report matches the one from the appliance.
  * if checksums are disabled, then it emits a warning to the log and continues.
  * @param vault Vault that contains the files
  * @param errorOnValidationFailure if true, fail the output if validation fails. If false, output a CopyReport with validationSuccess as false.
  */
class ValidateMD5[T](vault:Vault, errorOnValidationFailure:Boolean=false) extends GraphStage[FlowShape[CopyReport[T],CopyReport[T]]]{
  private final val in:Inlet[CopyReport[T]] = Inlet.create("ValidateMD5.in")
  private final val out:Outlet[CopyReport[T]] = Outlet.create("ValidateMD5.in")

  override def shape: FlowShape[CopyReport[T], CopyReport[T]] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private val logger = LoggerFactory.getLogger(getClass)

    setHandler(in, new AbstractInHandler {
      override def onPush(): Unit = {
        val elem = grab(in)

        val completedCb = createAsyncCallback[CopyReport[T]](report=>push(out,report))
        val failedCb = createAsyncCallback[Throwable](err=>fail(out, err))

        val mxsFile = vault.getObject(elem.oid)

        if(elem.checksum.isEmpty){
          logger.warn(s"Checksums disabled, can't validate output")
          push(out,elem)
        } else {
          MatrixStoreHelper.getOMFileMd5(mxsFile).onComplete({
            case Failure(err) =>
              logger.error(s"getOMFileMd5 crashed: ", err)
              failedCb.invoke(err)
            case Success(Failure(err)) =>
              logger.error(s"Could not get MD5 checksum from matrixstore: ", err)
            case Success(Success(md5)) =>
              logger.info(s"Got checksum $md5 from appliance, vs calculated ${elem.checksum}")
              if (md5.length != elem.checksum.get.length) {
                logger.error(s"Checksum types differ, this is a misconfiguration")
                failedCb.invoke(new RuntimeException("Checksum types differ, this is a misconfiguration"))
              } else {
                if (md5 != elem.checksum.get) {
                  logger.error(s"Checksums do not match!")
                  if (errorOnValidationFailure)
                    failedCb.invoke(new RuntimeException("Checksums do not match!"))
                  else
                    completedCb.invoke(elem.copy(validationPassed = Some(false)))
                } else {
                  completedCb.invoke(elem.copy(validationPassed = Some(true)))
                }
              }
          }) //getOMFileMd5.onComplete
        } //elem.checksum.isEmpty
      } //onPush
    }) //setHandler

    setHandler(out, new AbstractOutHandler {
      override def onPull(): Unit = pull(in)
    })
  } //new GraphStageLogic
} //ValidateMD5
