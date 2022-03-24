import akka.actor.ActorSystem
import akka.stream.{ClosedShape, Materializer}
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.helpers.{Copier, MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.mxscopy.streamcomponents.OMFastContentSearchSource
import com.gu.multimedia.storagetier.framework.{MessageProcessor, MessageProcessorReturnValue, RMQDestination, SilentDropMessage}
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
                          mxsConnectionBuilder: MXSConnectionBuilderImpl,
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
  def handleSuccessfulMetadataWrite(msg: Json):Future[Either[String, MessageProcessorReturnValue]] = msg.as[NearlineRecord] match {
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
                VidispineHelper.updateVidispineWithMXSId(itemId, updatedNearlineRecord)
              case None=>
                if(rec.expectingVidispineId) {
                  //VidispineMessageProcessor has been updated so that once a Vidispine import completes for an existing
                  //NearlineRecord then the MXS ID gets written. So we don't need to retry-loop here.
                  logger.info(s"The nearline record for ${rec.originalFilePath} does not have a vidispine id. The MXS ID" +
                    s" will be written when a Vidispine ingest completes.")
                  Future(Right(updatedNearlineRecord.asJson))
                } else {
                  logger.info(s"Not expecting ${rec.originalFilePath} to have a vidispine record")
                  Future.failed(SilentDropMessage(Some("Not expecting a vidispine record for this")))
                }
            }
        })
  }

  /**
   * Looks for all files that match the given filepath in MXFS_PATH and file size in _mxs_length or DPSP_SIZE
   * @param vault Vault object indicating the vault to query
   * @param completeFilePath file path to the original item
   * @param targetLength length of the original item
   * @return a Future, with a sequence of matching ObjectMatrixEntry instances for each matching file
   */
  def matchingFiles(vault:Vault, completeFilePath:String, targetLength:Long) = {
    val interestingFields = Array("MXFS_PATH, __mxs_length, DPSP_SIZE")

    val graph = GraphDSL.createGraph(Sink.seq[ObjectMatrixEntry]) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val src = builder.add(new OMFastContentSearchSource(vault, s"MXFS_PATH:\"$completeFilePath\"", interestingFields))
      src.out.filter(_.maybeGetSize().contains(targetLength)) ~> sink
      ClosedShape
    }

    RunnableGraph
      .fromGraph(graph)
      .run()
      .map(matches=>{
        logger.info(s"There are ${matches.length} files with the file name $completeFilePath and length $targetLength in vault ${vault.getId}")
        matches
      })
  }

  /**
   * Queries the destination vault to see if we already have at least one entry of the given name and of the file size
   * of the given object in the source vault
   * @param nearlineVault source vault, the vault corresponding to the objectId in `rec`
   * @param destVault destination vault, the vault to query for matching files
   * @param rec NearlineRecord that indicates the file we are going to copy
   * @return a Future, containing True if we need to copy and False if we don't.
   */
  def isCopyNeeded(nearlineVault:Vault, destVault:Vault, rec:NearlineRecord) = {
    for {
      fileSize <- Future.fromTry(Try { nearlineVault.getObject(rec.objectId) }.map(MetadataHelper.getFileSize))
      destFiles <- matchingFiles(destVault, rec.originalFilePath, fileSize)
    } yield destFiles.isEmpty //if there are no files matching this name and size then we _do_ want to copy
  }

  /**
   * calls out to copier to perform an appliance-to-appliance copy.
   * included as a seperate method to make test mocking easier.
   * @param nearlineVault vault to copy from
   * @param entry ObjectMatrixEntry representing the entry to copy
   * @param destVault vault to copy to
   * @return a Future, containing a tuple of the written object ID and an optional MD5 checksum
   */
  protected def callCrossCopy(nearlineVault:Vault, sourceOID:String, destVault:Vault) = Copier
    .doCrossCopy(nearlineVault, sourceOID, destVault)

  def handleInternalArchiveRequested(msg: Json):Future[Either[String, MessageProcessorReturnValue]] = msg.as[NearlineRecord] match {
    case Left(err)=>
      Future.failed(new RuntimeException(s"Could not parse message as a nearline record: $err"))
    case Right(rec)=>
      mxsConfig.internalArchiveVaultId match {
        case Some(internalArchiveVaultId) =>
          //this message has been output by `applyCustomMetadata` above. So we can assume that (a) the source object ID in the message is
          //valid, and (b) that it has the right metadata to copy from. All we need to do is to kick off the copy.
          mxsConnectionBuilder.withVaultsFuture(Seq(mxsConfig.nearlineVaultId, internalArchiveVaultId)) { vaults =>
            val nearlineVault = vaults.head
            val internalArchiveVault = vaults(1)

            isCopyNeeded(nearlineVault, internalArchiveVault, rec).flatMap({
              case true =>
                callCrossCopy(nearlineVault, rec.objectId, internalArchiveVault)
                  .flatMap(writtenOid => {
                    logger.info(s"Copied from ${rec.objectId} to $writtenOid for ${rec.originalFilePath}")
                    nearlineRecordDAO
                      .setInternallyArchived(rec.id.get, true)
                      .map({
                        case Some(updatedRec) =>
                          Right(updatedRec.asJson)
                        case None =>
                          throw new RuntimeException(s"Record id ${rec.id} is not valid!")
                      })
                  })
                  .recover({
                    case err: Throwable => //handle a copy error as a retryable failure, likelihood is that it's to do with appliance load.
                      logger.error(s"Could not copy entry ${rec.objectId} onto vault $internalArchiveVaultId: ${err.getMessage}", err)
                      Left(err.getMessage)
                  })
              case false=>
                logger.info(s"${rec.originalFilePath} already exists in the archive vault, no copy is needed")
                Future.failed(SilentDropMessage(Some(s"${rec.originalFilePath} is already archived")))
            })
          }
        case None=>
          logger.error(s"The internal archive vault ID has not been configured, so it's not possible to send an item to internal archive.")
          Future.failed(new RuntimeException(s"Internal archive vault not configured"))
      }
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
    case "storagetier.nearline.newfile.success"=>           //notification of successful media copy = GP-598
      handleSuccessfulMediaCopy(msg)
    case "storagetier.nearline.metadata.success"=>          //notification that objectmatrix metadata has been written = GP-627
      handleSuccessfulMetadataWrite(msg)
    case "storagetier.nearline.internalarchive.required"=>  //notification that a (now existing file) needs internal archive = GP-599
      handleInternalArchiveRequested(msg)
    case _=>
      Future(Left(s"Unrecognised routing key: $routingKey"))
  }
}
