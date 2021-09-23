package archivehunter

import java.time.ZonedDateTime

object ArchiveHunterResponses {
  case class ArchiveHunterGetResponse[T:io.circe.Decoder](
                                                         status:String,
                                                         entityType:String,
                                                         entity:T
                                                         )

  /**
   * partial ArchiveEntry definition, skipping out fields which we are not particularly interested in and are complex
   * @param id
   * @param bucket
   * @param path
   * @param region
   * @param file_extension
   * @param size
   * @param last_modified
   * @param etag
   * @param mimeType
   * @param proxied
   * @param storageClass
   * @param beenDeleted
   */
  case class ArchiveEntry(id:String, bucket: String, path: String, region:Option[String], file_extension: Option[String],
                          size: scala.Long, last_modified: ZonedDateTime, etag: String, mimeType: String,
                          proxied: Boolean, storageClass:String,
                          beenDeleted:Boolean=false)

  type ArchiveHunterEntryResponse = ArchiveHunterGetResponse[ArchiveEntry]
}
