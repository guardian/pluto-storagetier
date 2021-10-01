package messages

import java.time.ZonedDateTime
/*
{
    "field": [
        {
            "key": "item",
            "value": "VX-3986"
        },
        {
            "key": "shapeIds",
            "value": "VX-5982"
        },
        {
            "key": "transcodeDurations",
            "value": "18423667@1000000"
        },
        {
            "key": "transcodeEstimatedTimeLeft",
            "value": "0.0"
        },
        {
            "key": "transcodeMediaTimes",
            "value": "886784@48000"
        },
        {
            "key": "transcodeProgress",
            "value": "100.0"
        },
        {
            "key": "transcodeWallTime",
            "value": "71.1195"
        }
    ]
}
*/

case class VidispineField(key: String, value: String)

case class VidispineMediaIngested(field: List[VidispineField]) {
  def originalPath: Option[String] = getValue("originalPath")
  def itemId: Option[String] = getValue("itemId")
  def fileSize: Option[Long] = getValue("bytesWritten").getOrElse("-1").toLongOption
  def status: Option[String] = getValue("status")

  private def getValue(fieldKey: String) = field.find(field=>field.key == fieldKey).map(field=>field.value)
}