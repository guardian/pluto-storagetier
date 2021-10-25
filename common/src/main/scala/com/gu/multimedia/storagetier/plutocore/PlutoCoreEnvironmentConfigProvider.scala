package com.gu.multimedia.storagetier.plutocore

import java.nio.file.Paths
import scala.util.{Failure, Success, Try}

class PlutoCoreEnvironmentConfigProvider extends PlutoCoreConfigProvider {
  private def stringToBool(str:String):Boolean = {
    val downcased = str.toLowerCase
    if(downcased=="true"||downcased=="yes") true else false
  }
  
  override def get(): Either[String, PlutoCoreConfig] = {
    (sys.env.get("PLUTO_CORE_BASEURI"), sys.env.get("PLUTO_CORE_SECRET"), sys.env.get("ASSET_FOLDER_BASEPATH"), sys.env.get("PLUTO_CORE_DISABLE").map(stringToBool)) match {
      case (Some(uri), Some(secret), Some(assetFolderBaseStr), maybeDisable)=>
        Try { Paths.get(assetFolderBaseStr) } match {
          case Success(path)=>
            Right(PlutoCoreConfig(uri,secret, path, !maybeDisable.getOrElse(false)))
          case Failure(err)=>
            Left(err.getMessage)
        }

      case (_, _, _,_) => Left("You must specify PLUTO_CORE_BASEURI, PLUTO_CORE_SECRET and ASSET_FOLDER_BASEPATH")
    }
  }
}
