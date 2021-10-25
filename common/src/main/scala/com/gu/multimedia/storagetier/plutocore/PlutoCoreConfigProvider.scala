package com.gu.multimedia.storagetier.plutocore

trait PlutoCoreConfigProvider {
  def get():Either[String,PlutoCoreConfig]
}

