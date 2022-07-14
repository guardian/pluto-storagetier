package messages

case class MediaRemovedMessage (mediaTier: String,
                               filePath: Option[String],
                               itemId: Option[String],
                               nearlineId: String)
