package messages

case class MediaRemovedMessage(mediaTier: String,
                               originalFilePath: String,
                               vidispineItemId: Option[String],
                               nearlineId: Option[String])
