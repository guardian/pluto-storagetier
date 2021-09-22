package messages

import java.time.ZonedDateTime

case class AssetSweeperNewFile(
                              imported_id:Option[String],
                              size:Long,
                              ignore:Boolean,
                              mime_type: String,
                              mtime: ZonedDateTime,
                              ctime: ZonedDateTime,
                              atime: ZonedDateTime,
                              owner: String,
                              group: String,
                              filepath: String,
                              filename: String
                              )
