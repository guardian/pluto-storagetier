package com.gu.multimedia.storagetier.models.online_archive

import slick.jdbc.PostgresProfile.api._

case class IgnoredRecord (id:Option[Int], originalFilePath:String, ignoreReason: String, vidispineItemId:Option[String], vidispineVersionId:Option[Int])

class IgnoredRecordRow(tag:Tag) extends Table[IgnoredRecord](tag, "onlinearchive_ignored_record") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def originalFilePath = column[String]("s_original_filepath")
  def ignoreReason = column[String]("s_ignore_reason")
  def vidispineItemId = column[Option[String]]("s_vidispine_item_id")
  def vidispineVersionId = column[Option[Int]]("s_vidispine_version_id")

  def filePathIndex = index("filepath_unique", originalFilePath, unique = true)
  def * = (id.?, originalFilePath, ignoreReason, vidispineItemId, vidispineVersionId) <> (IgnoredRecord.tupled, IgnoredRecord.unapply)
}
