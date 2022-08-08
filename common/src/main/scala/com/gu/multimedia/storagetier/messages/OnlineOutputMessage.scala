package com.gu.multimedia.storagetier.messages

case class OnlineOutputMessage(mediaTier: String,
                               projectIds: Seq[Int],
                               originalFilePath: Option[String],
                               fileSize: Option[Long],
                               vidispineItemId: Option[String],
                               nearlineId: Option[String],
                               mediaCategory: String)
