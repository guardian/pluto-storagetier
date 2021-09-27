package archivehunter

trait ArchiveHunterConfigProvider {
  def get():Either[String, ArchiveHunterConfig]
}
