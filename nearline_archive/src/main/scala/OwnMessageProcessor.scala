import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.storagetier.framework.MessageProcessor
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, CommissionRecord, PlutoCoreConfig, ProjectRecord, WorkingGroupRecord}
import com.om.mxs.client.japi.{MxsObject, Vault}
import io.circe.Json
import io.circe.generic.auto._
import matrixstore.{CustomMXSMetadata, MatrixStoreConfig}
import org.slf4j.LoggerFactory
import io.circe.syntax._

import java.nio.file.Paths
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

class OwnMessageProcessor(mxsConfig:MatrixStoreConfig, asLookup:AssetFolderLookup)(implicit mat:Materializer, ec:ExecutionContext, actorSystem:ActorSystem, mxsConnectionBuilder: MXSConnectionBuilder) extends MessageProcessor {
  import com.gu.multimedia.storagetier.plutocore.ProjectRecordEncoder._
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Build a vaultdoor-compatible metadata set based on incoming pluto-core data.
   * TODO: extend this to handle "deliverable" type as well as "rushes" type information
   * @param maybeProject ProjectRecord, can be None
   * @param maybeCommission, CommissionRecord, can be None
   * @param maybeWG WorkingGroupRecord, can be None
   * @return a CustomMXSMetadata object
   */
  def generateMetadata(maybeProject:Option[ProjectRecord], maybeCommission:Option[CommissionRecord], maybeWG:Option[WorkingGroupRecord], filename:String):CustomMXSMetadata = {
    if(maybeProject.isEmpty) {
      logger.warn(s"Could not find project information for file $filename")
    }
    if(maybeCommission.isEmpty) {
      logger.warn(s"Could not find commission information for file $filename")
    }
    if(maybeWG.isEmpty) {
      logger.warn(s"Could not find working-group information for file $filename")
    }

    CustomMXSMetadata(
      CustomMXSMetadata.TYPE_RUSHES,
      maybeProject.flatMap(_.id).map(_.toString),
      maybeProject.flatMap(_.commissionId).map(_.toString),
      None,                       //masterId
      None,                       //masterName
      None,                       //masterUser
      maybeProject.map(_.title),  //projectName
      maybeCommission.map(_.title), //commissionName
      maybeWG.map(_.name),
      None,                     //deliverableAssetId
      None,                     //deliverableBundle
      None,                     //deliverableVersion
      None                      //deliverableType
    )
  }

  protected def writeMetadataToObject(mxsObject: MxsObject, md:MxsMetadata, rec:NearlineRecord) =
    Try {
      logger.debug(s"Writing metadata to ${mxsObject.getId} for file ${rec.originalFilePath}")
      val view = mxsObject.getAttributeView
      view.writeAllAttributes(md.toAttributes.asJava)
    } match {
      case Success(_)=>
        //we wrote the attributes down to the appliance successfully
        Right(rec)
      case Failure(err)=>
        logger.error(s"Could not write attributes to object ID ${rec.objectId}: ${err.getMessage}", err)
        Left(err.getMessage)
    }

  def applyCustomMetadata(rec:NearlineRecord, vault:Vault) = {
    //generate vaultdoor-compatible metadata
    val mdFuture = for {
      maybeProject <- asLookup.assetFolderProjectLookup(Paths.get(rec.originalFilePath))  //check - is this safe? might Paths.get raise an exception?
      maybeCommission <- asLookup.optionCommissionLookup(maybeProject.flatMap(_.commissionId))
      maybeWg <- asLookup.optionWGLookup(maybeCommission.map(_.workingGroupId))
      mxsData <- Future(generateMetadata(maybeProject, maybeCommission, maybeWg, rec.originalFilePath))
    } yield mxsData.toAttributes(MxsMetadata.empty)

    //write the attributes onto the file, if successful
    mdFuture.map(md=>{
      Try { vault.getObject(rec.objectId) } match {
        case Failure(err)=>
          logger.error(s"Could not get object with ID ${rec.objectId} for correlation id ${rec.id}: $err", err)
          Left(err.getMessage)
        case Success(mxsObject)=>
          writeMetadataToObject(mxsObject, md, rec)
      }
    }).recover({
      case err:Throwable=>
        logger.error(s"Lookup of metadata failed for ${rec.originalFilePath} (${rec.objectId}): ${err.getMessage}", err)
        Left(err.getMessage)
    })
  }

  def handleSuccessfulMediaCopy(msg: Json) = msg.as[NearlineRecord] match {
    case Left(err)=>
      Future.failed(new RuntimeException(s"Could not parse message as a nearline record: $err"))
    case Right(rec)=>
      mxsConnectionBuilder.withVaultFuture(mxsConfig.nearlineVaultId) { vault=>
        applyCustomMetadata(rec, vault)
      }.andThen({
        case Success(Left(err))=>
          logger.warn(s"Could not process message for ${rec.originalFilePath} due to retryable fault: $err")
        case Success(Right(_))=>
          logger.info(s"Successfully processed copy-success message for ${rec.originalFilePath}")
        case Failure(err)=>
          logger.error(s"Could not process message for ${rec.originalFilePath} due to fatal error: $err")
      })
  }

  /**
   * @param routingKey the routing key of the message as received from the broker.
   * @param msg        the message body, as a circe Json object. You can unmarshal this into a case class by
   *                   using msg.as[CaseClassFormat]
   * @return You need to return Left() with a descriptive error string if the message could not be processed, or Right
   *         with a circe Json body (can be done with caseClassInstance.noSpaces) containing a message body to send
   *         to our exchange with details of the completed operation
   */
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, Json]] = routingKey match {
    case "storagetier.nearlinearchive.newfile.success"=>  //notification of successful media copy = GP-598
      handleSuccessfulMediaCopy(msg).map(_.map(_.asJson))
  }
}
