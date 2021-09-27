package com.gu.multimedia.storagetier.models.online_archive

import slick.jdbc.PostgresProfile.api._

/*
{
"id":12345,
"archiveHunterID": "abcdefg",
"archiveHunterIDValidated": false,
"originalFilePath": "/original/path/to/file.ext",
"uploadedBucket": "somebucket",
"uploadedPath": "path/to/file.ext",
}
 */
case class ArchivedRecord(id:Option[Int],
                          archiveHunterID:String,
                          archiveHunterIDValidated:Boolean,
                          originalFilePath:String,
                          uploadedBucket:String,
                          uploadedPath:String,
                          uploadedVersion:Option[Int],
                          vidispineItemId:Option[String],
                          vidispineVersionId:Option[Int],
                          proxyBucket:Option[String],
                          proxyPath:Option[String],
                          proxyVersion:Option[Int],
                          metadataXML:Option[String],
                          metadataVersion:Option[Int]
                         )

object ArchivedRecord extends ((Option[Int], String, String, String, String, Option[Int], Option[String], Option[Int], Option[String], Option[String], Option[Int], Option[String], Option[Int]) => ArchivedRecord ){
  def apply(archiveHunterID:String, originalFilePath:String, uploadedBucket:String, uploadedPath:String, uploadedVersion:Option[Int]) = {
    new ArchivedRecord(None, archiveHunterID, originalFilePath, uploadedBucket, uploadedPath, uploadedVersion, None,None,None,None,None,None,None)
  }
}

class ArchivedRecordRow (tag:Tag) extends Table[ArchivedRecord](tag, "onlinearchive_archived_record") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def archiveHunterID = column[String]("s_archivehunter_id")
  def archiveHunterIDValidated = column[Boolean]("b_archivehunter_id_validated")
  def originalFilePath = column[String]("s_original_filepath")
  def uploadedBucket = column[String]("s_uploaded_bucket")
  def uploadedPath = column[String]("s_uploaded_path")
  def uploadedVersion = column[Option[Int]]("i_uploaded_version")
  def vidispineItemId = column[Option[String]]("s_vidispine_itemid")
  def vidispineVersionId = column[Option[Int]]("i_vidispine_versionid")
  def proxyBucket = column[Option[String]]("s_proxy_bucket")
  def proxyPath = column[Option[String]]("s_proxy_path")
  def proxyVersion = column[Option[Int]]("i_proxy_version")
  def metadataXML = column[Option[String]]("s_metadata_xml_path")
  def metadataVersion = column[Option[Int]]("i_metadata_version")

  def filepathIdx = index("filepath_idx", originalFilePath)
  def archiveHunterIdIds = index("archivehunter_id_idx", archiveHunterID, unique = true)
  def vidispineIdIdx = index("vidispine_item_idx", vidispineItemId)

  def * = (id.?, archiveHunterID, archiveHunterIDValidated, originalFilePath, uploadedBucket, uploadedPath, uploadedVersion, vidispineItemId, vidispineVersionId, proxyBucket, proxyPath, proxyVersion, metadataXML, metadataVersion) <> (ArchivedRecord.tupled, ArchivedRecord.unapply)
}
