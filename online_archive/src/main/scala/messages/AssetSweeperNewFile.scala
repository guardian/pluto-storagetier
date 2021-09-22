package messages

import java.time.ZonedDateTime

/*
{
"size": 12345,
"ignore": false,
"mime_type": "video/mp4",
"mtime": "2021-01-02T03:04:05Z",
"ctime": "2021-01-02T03:04:05Z",
"atime": "2021-01-02T03:04:05Z",
"owner": "100",
"group": "100",
"filepath": "/srv/media/working-group/commission/project/footage",
"filename": "test.mp4"
}
 */
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
