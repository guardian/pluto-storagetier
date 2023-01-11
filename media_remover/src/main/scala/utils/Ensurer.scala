package utils

object Ensurer {
  def validateNeededFields(fileSizeMaybe: Option[Long], filePathMaybe: Option[String], nearlineOrOnlineIdMaybe: Option[String]): (Long, String, String) =
    (fileSizeMaybe, filePathMaybe, nearlineOrOnlineIdMaybe) match {
      case (None, _, _) => throw new RuntimeException(s"fileSize is missing")
      case (_, None, _) => throw new RuntimeException(s"filePath is missing")
      case (_, _, None) => throw new RuntimeException(s"media id is missing")
      case (Some(-1), _, _) => throw new RuntimeException(s"fileSize is -1")
      case (Some(fileSize), Some(filePath), Some(id)) => (fileSize, filePath, id)
    }

  def validateMediaId(nearlineOrOnlineIdMaybe: Option[String]): String =
    nearlineOrOnlineIdMaybe match {
      case Some(id) => id
      case None => throw new RuntimeException(s"media id is missing")
    }
}
