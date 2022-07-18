package com.gu.multimedia.storagetier.messages

case class MultiProjectOnlineOutputMessage(mediaTier: String,
                                           projectIds: Seq[Int],
                                           filePath: Option[String],
                                           fileSize: Option[Long],
                                           itemId: Option[String],
                                           nearlineId: String,
                                           mediaCategory: String)
