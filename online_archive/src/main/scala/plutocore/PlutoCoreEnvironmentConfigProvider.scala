package plutocore

import java.nio.file.Paths
import scala.util.{Failure, Success, Try}

class PlutoCoreEnvironmentConfigProvider extends PlutoCoreConfigProvider {
  override def get(): Either[String, PlutoCoreConfig] = {
    (sys.env.get("PLUTO_CORE_BASEURI"), sys.env.get("PLUTO_CORE_SECRET"), sys.env.get("ASSET_FOLDER_BASEPATH")) match {
      case (Some(uri), Some(secret), Some(assetFolderBaseStr))=>
        Try { Paths.get(assetFolderBaseStr) } match {
          case Success(path)=>
            Right(PlutoCoreConfig(uri,secret, path))
          case Failure(err)=>
            Left(err.getMessage)
        }

      case (_, _, _) => Left("You must specify PLUTO_CORE_BASEURI, PLUTO_CORE_SECRET and ASSET_FOLDER_BASEPATH")
    }
  }
}
