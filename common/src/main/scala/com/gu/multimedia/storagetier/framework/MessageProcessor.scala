package com.gu.multimedia.storagetier.framework

import io.circe.Json

trait MessageProcessor {
  def handleMessage(msg:Json):Either[String,Json]
}

