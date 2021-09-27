package archivehunter

class ArchiveHunterEnvironmentConfigProvider extends ArchiveHunterConfigProvider {

  override def get(): Either[String, ArchiveHunterConfig] = {
    (sys.env.get("ARCHIVE_HUNTER_BASEURI"), sys.env.get("ARCHIVE_HUNTER_SHARED_SECRET")) match {
      case (Some(baseUri), Some(sharedSecret))=>
        Right(ArchiveHunterConfig(baseUri, sharedSecret))
      case (_,_)=>
        Left("You must specify ARCHIVE_HUNTER_BASEURI and ARCHIVE_HUNTER_SHARED_SECRET")
    }
  }
}
