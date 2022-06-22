package messages

import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.storagetier.vidispine.{SearchResultItemSimplified, VSOnlineOutputMessage}
/**
 * Converts an ObjectMatrixEntry object or Vidispine object to an OnlineOutputMessage message
 * for every associated file in a project
 * */

case class OnlineOutputMessage(mediaTier: String,
                               projectId: Int,
                               filePath: Option[String],
                               itemId: Option[String],
                               nearlineId: String,
                               mediaCategory: String)
object OnlineOutputMessage {
  def apply(file: ObjectMatrixEntry): OnlineOutputMessage = {
    (file.intAttribute("GNM_PROJECT_ID"), file.stringAttribute("GNM_TYPE")) match {
      case (Some(projectId), Some(gnmType))=>
        new OnlineOutputMessage(
          "NEARLINE",
          projectId,
          file.pathOrFilename,
          file.stringAttribute("GNM_VIDISPINE_ITEM"),
          file.oid,
          gnmType
        )
      case _=>
        throw new RuntimeException(s"Objectmatrix file ${file.oid} is missing either GNM_PROJECT_ID or GNM_TYPE fields")
    }
  }

  def fromResponseItem(
                        itemSimplified: SearchResultItemSimplified,
                        projectId: Int
                      ): Option[OnlineOutputMessage] = {
    val itemId = Option(itemSimplified.id)
    val filePath =
      itemSimplified.item.shape.head.getLikelyFile.flatMap(_.getAbsolutePath)
    val nearlineId = itemSimplified
      .valuesForField("gnm_nearline_id", Some("Asset"))
      .headOption
      .map(_.value)
    val mediaCategory = itemSimplified
      .valuesForField("gnm_category", Some("Asset"))
      .headOption
      .map(_.value)
    (nearlineId, mediaCategory) match {
      case (Some(nearlineId), Some(mediaCategory)) =>
        Some(
          OnlineOutputMessage(
            "ONLINE",
            projectId,
            filePath,
            itemId,
            nearlineId,
            mediaCategory
          )
        )
      case _ =>
        None
    }
  }
}
