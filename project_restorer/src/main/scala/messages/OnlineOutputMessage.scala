package messages

import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry

case class OnlineOutputMessage(mediaTier: String,
                               projectId: Int,
                               filePath: Option[String],
                               itemId: Option[String],
                               nearlineId: Option[String],
                               nearlineVersion: Option[Int],
                               mediaCategory: String)
object OnlineOutputMessage {
  def apply(file: ObjectMatrixEntry): OnlineOutputMessage = {
    new OnlineOutputMessage(
      "NEARLINE",
      file.intAttribute("GNM_PROJECT_ID")[Int],
      file.pathOrFilename,
      file.stringAttribute("GNM_VIDISPINE_ITEM")[String],
      file.oid[String],
      file.oid.toIntOption,
      file.stringAttribute("GNM_TYPE")[String]
    )
  }
}