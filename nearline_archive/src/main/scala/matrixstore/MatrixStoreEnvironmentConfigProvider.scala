package matrixstore

class MatrixStoreEnvironmentConfigProvider extends MatrixStoreConfigProvider {
  private def stringToArray(str:String):Array[String] = {
    str.split("\\s*,\\s*")
  }

  override def get(): Either[String, MatrixStoreConfig] = {
    (sys.env.get("MATRIX_STORE_ACCESS_KEY_ID"), sys.env.get("MATRIX_STORE_ACCESS_KEY_SECRET"),
      sys.env.get("MATRIX_STORE_HOSTS").map(stringToArray)) match {
      case (Some(accessKeyId), Some(accessKeySecret), Some(hosts))=>
        Right(MatrixStoreConfig(hosts, accessKeyId, accessKeySecret))
      case (_,_,_)=>
        Left("You must specify MATRIX_STORE_ACCESS_KEY_ID, MATRIX_STORE_ACCESS_KEY_SECRET and MATRIX_STORE_HOSTS (comma separated " +
          "string for several hosts)")
    }
  }
}
