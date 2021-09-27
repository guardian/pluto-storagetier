package archivehunter

import java.time.ZonedDateTime

object ArchiveHunterResponses {
  case class ArchiveHunterGetResponse[T:io.circe.Decoder](
                                                         status:String,
                                                         objectClass:String,
                                                         entry:T
                                                         )
  case class MimeType(major:String, minor:String)
  /**
   * partial ArchiveEntry definition, skipping out fields which we are not particularly interested in and are complex
   * @param id ArchiveHunter ID of the record
   * @param bucket  bucket that the item belongs in
   * @param path    path that the media file lives at in the bucket
   * @param region  region of the bucket
   * @param file_extension file extension of the file, if it has one
   * @param size total data size of the file
   * @param last_modified zoned date time of the last modification sound
   * @param etag S3 etag of the file
   * @param mimeType detected MIME type of the content
   * @param proxied true if a proxy should exist
   * @param storageClass S3 StorageClass of the data
   * @param beenDeleted true if the original media has been deleted
   */
  case class ArchiveEntry(id:String, bucket: String, path: String, region:Option[String], file_extension: Option[String],
                          size: scala.Long, last_modified: ZonedDateTime, etag: String, mimeType: MimeType,
                          proxied: Boolean, storageClass:String,
                          beenDeleted:Boolean=false)

  type ArchiveHunterEntryResponse = ArchiveHunterGetResponse[ArchiveEntry]
}
