package com.gu.multimedia.storagetier.plutocore

import java.nio.file.Path

case class PlutoCoreConfig(baseUri:String, sharedSecret:String, assetFolderBasePath:Path, enabled:Boolean=true)
