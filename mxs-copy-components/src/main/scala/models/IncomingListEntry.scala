package models

import java.time.ZonedDateTime

import io.circe.Json

import scala.util.Try
case class IncomingListEntry(filePath:String, fileName:String, mtime:ZonedDateTime, size:Long) extends Incoming
{
  override def filepath = s"$filePath/$fileName"
}

object IncomingListEntry {
  def fromJson(j:Json) = Try {
    new IncomingListEntry(
      (j \\ "filepath").head.asString.getOrElse("none"),
      (j \\ "filename").head.asString.getOrElse("none"),
      ZonedDateTime.parse((j \\ "mtime").head.asString.getOrElse("none")),
      (j \\ "size").head.asNumber.flatMap(_.toLong).getOrElse(-1L)
    )
  }
}