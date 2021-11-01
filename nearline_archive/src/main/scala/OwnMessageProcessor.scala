import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilder
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.mxscopy.models.MxsMetadata
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, RMQDestination}
import com.gu.multimedia.storagetier.models.nearline_archive.{NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, CommissionRecord, PlutoCoreConfig, ProjectRecord, WorkingGroupRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
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

class OwnMessageProcessor(mxsConfig:MatrixStoreConfig, asLookup:AssetFolderLookup, ownExchangeName:String)
                         (implicit mat:Materializer,
                          ec:ExecutionContext,
                          actorSystem:ActorSystem,
                          mxsConnectionBuilder: MXSConnectionBuilder,
                          vsCommunicator:VidispineCommunicator,
                          nearlineRecordDAO: NearlineRecordDAO) extends MessageProcessor {
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
    } yield (mxsData.toAttributes(MxsMetadata.empty), maybeProject.flatMap(_.sensitive))

    //write the attributes onto the file, if successful
    mdFuture.map(result=>{
      val md = result._1
      val maybeSensitive = result._2
      Try { vault.getObject(rec.objectId) } match {
        case Failure(err)=>
          logger.error(s"Could not get object with ID ${rec.objectId} for correlation id ${rec.id}: $err", err)
          Left(err.getMessage)
        case Success(mxsObject)=>
          writeMetadataToObject(mxsObject, md, rec)
            .map(rec=>{
              val extraSendLocations = if(maybeSensitive.contains(true)) {
                Seq(RMQDestination(ownExchangeName, "storagetier.nearline.internalarchive.required"))
              } else {
                Seq()
              }
              MessageProcessorReturnValue(rec.asJson, extraSendLocations)
            })
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
   * once we have written the metadata, we need to update Vidispine to say where the file is on the nearline
   * @param msg circe Json object representing the (potentially outdated) NearlineRecord
   * @return a failed Future if a non-retryable error occurred, a LEft with a descriptive string if a retryable error
   *         occurred or a Right with a Json object (automatically upcast to MessageProcessorReturnValue) containing
   *         the updated NearlineRecord if successful
   */
  def handleSuccessfulMetadataWrite(msg: Json) = msg.as[NearlineRecord] match {
    case Left(err)=>
      Future.failed(new RuntimeException(s"Could not parse message as a nearline record: $err"))
    case Right(rec)=>
      import cats.implicits._
      //because this message might arrive _before_ the vidispine item id has been set, we need to get the _current_
      //state of the item from the datastore and not rely on the state from the message
      //".sequence" here is a bit of cats magic that converts Option[Future[Option[A]]] into Future[Option[Option[A]]]
      rec.id
        .map(recId=>nearlineRecordDAO.getRecord(recId))
        .sequence
        .map(_.flatten)
        .flatMap({
          case None=>
            logger.error(s"Can't update vidispine record for item ${rec.originalFilePath} " +
              s"because the record ${rec.id} does not exist in the storagetier nearline records")
            Future.failed(new RuntimeException(s"Record ${rec.id} does not exist in nearline records"))
          case Some(updatedNearlineRecord)=>
            updatedNearlineRecord.vidispineItemId match {
              case Some(itemId)=>
                logger.info(s"Updating metadata on $itemId to set nearline object id ${updatedNearlineRecord.objectId}")
                vsCommunicator.setGroupedMetadataValue(itemId, "Asset", "gnm_nearline_id", updatedNearlineRecord.objectId).map({
                  case None=>
                    logger.error(s"Item $itemId does not exist in vidispine! (got a 404 trying to update metadata)")
                    throw new RuntimeException(s"Item $itemId does not exist in Vidispine")
                  case Some(_)=>
                    logger.info(s"Successfully updated metadata on $itemId")
                    Right(updatedNearlineRecord.asJson)
                }).recover({
                  case err:Throwable=>
                    logger.error(s"Could not update item $itemId in Vidispine: ${err.getMessage}", err)
                    Left(s"Could not update item $itemId in Vidispine")
                })
              case None=>
                logger.info(s"The nearline record for ${rec.originalFilePath} does not have a vidispine id (yet)")
                Future(Left("No vidispine id yet"))
            }
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
  override def handleMessage(routingKey: String, msg: Json): Future[Either[String, MessageProcessorReturnValue]] = routingKey match {
    case "storagetier.nearline.newfile.success"=>  //notification of successful media copy = GP-598
      handleSuccessfulMediaCopy(msg)
    case "storagetier.nearline.metadata.success"=>  //notification that objectmatrix metadata has been written = GP-627
      handleSuccessfulMetadataWrite(msg)
  }
}
