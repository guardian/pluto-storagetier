package messages

import java.time.ZonedDateTime
import scala.util.Try
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
  def filePath: Option[String] = getFilePath
  def itemId: Option[String] = getValue("itemId")
  def fileSize: Option[Long] = getValue("bytesWritten").flatMap(value=>Try { value.toLong }.toOption)
  def status: Option[String] = getValue("status")
  def essenceVersion: Option[Int] = getValue("essenceVersion").flatMap(value => Try { value.toInt }.toOption)
  def sourceFileId:Option[String] = getValue("sourceFileId")
  def sourceFileIds:Array[String] = getValue("sourceFileIds")
    .map(_.split(",")).getOrElse[Array[String]](Array()) //FIXME: I assume that the separator is a , ?

  private def getValue(fieldKey: String) = field.find(field=>field.key == fieldKey).map(field=>field.value)
  private def getFilePath() = {
    val sourceFileId = getValue("sourceFileId")

    val fileRefs = getValue("filePathMap").map(_.split(",").flatMap(FileIdPair.fromString))
    fileRefs
      .flatMap(_
        .filter(fieldPair => sourceFileId.contains(fieldPair.vsFileId))
        .map(_.relativePath)
        .headOption
      )
  }
}

case class FileIdPair (vsFileId:String, relativePath:String)

object FileIdPair extends ((String, String)=>FileIdPair) {
  def fromString(source:String):Option[FileIdPair] = {
    val parts = source.split("=")
    if(parts.length==2) {
      Some(new FileIdPair(parts.head, parts(1)))
    } else {
      None
    }
  }
}