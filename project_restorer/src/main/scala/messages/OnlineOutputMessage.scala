package messages

import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.storagetier.vidispine.{SearchResultItemSimplified, VSOnlineOutputMessage}
/**
 * Converts an ObjectMatrixEntry object or Vidispine object to an OnlineOutputMessage message
 * for every associated file in a project
 * */

case class OnlineOutputMessage(mediaTier: String,
                               projectId: String,
                               filePath: Option[String],
                               itemId: Option[String],
                               nearlineId: String,
                               mediaCategory: String)
object OnlineOutputMessage {
  def apply(file: ObjectMatrixEntry): OnlineOutputMessage = {
    (file.stringAttribute("GNM_PROJECT_ID"), file.stringAttribute("GNM_TYPE")) match {
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

  def apply(file: VSOnlineOutputMessage): OnlineOutputMessage = {
    new OnlineOutputMessage(
      file.mediaTier,
      file.projectId.toString,
      file.filePath,
      file.itemId,
      file.nearlineId,
      file.mediaCategory)
  }
}
