import com.gu.multimedia.storagetier.framework.MessageProcessorReturnValue
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecord
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import org.slf4j.LoggerFactory
import io.circe.syntax._
import io.circe.generic.auto._

import scala.concurrent.{ExecutionContext, Future}

object VidispineHelper {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Writes the Matrixstore object ID onto the metadata of the vidispine item given by itemId
   * @param itemId Vidispine item ID
   * @param updatedNearlineRecord NearlineRecord indicating the objectid that needs to be set
   * @param vsCommunicator implicitly provided VidispineCommunicator instance
   * @param ec implicitly provided ExecutionContext
   * @return a Future, which fails on "permanent" error, returns a Right containing the nearline record converted to Circe
   *         JSON on success or a Left containing an error string on "temporary" error
   */
  def updateVidispineWithMXSId(itemId:String, updatedNearlineRecord:NearlineRecord)(implicit vsCommunicator:VidispineCommunicator, ec:ExecutionContext):Future[Either[String, MessageProcessorReturnValue]] = {
    logger.info(s"Updating metadata on $itemId to set nearline object id ${updatedNearlineRecord.objectId}")
    vsCommunicator.setGroupedMetadataValue(itemId, "Asset", "gnm_nearline_id", updatedNearlineRecord.objectId).map({
      case None=>
        logger.error(s"Item $itemId does not exist in vidispine! (got a 404 trying to update metadata)")
        throw new RuntimeException(s"Item $itemId does not exist in Vidispine")
      case Some(_)=>
        logger.info(s"Successfully updated metadata on $itemId")
        Right(MessageProcessorReturnValue(updatedNearlineRecord.asJson))
    }).recover({
      case err:Throwable=>
        logger.error(s"Could not update item $itemId in Vidispine: ${err.getMessage}", err)
        Left(s"Could not update item $itemId in Vidispine")
    })
  }
}
