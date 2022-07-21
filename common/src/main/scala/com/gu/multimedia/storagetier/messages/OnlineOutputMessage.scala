package com.gu.multimedia.storagetier.messages

case class OnlineOutputMessage(mediaTier: String,
                               projectIds: Seq[String],
                               filePath: Option[String],
                               fileSize: Option[Long],
                               itemId: Option[String],
                               nearlineId: Option[String],
                               mediaCategory: String)
