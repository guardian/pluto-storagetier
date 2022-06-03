package messages

import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry

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
}