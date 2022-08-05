package com.gu.multimedia.storagetier.models.media_remover

import com.gu.multimedia.storagetier.models.common.MediaTiers
import slick.jdbc.PostgresProfile.api._

case class PendingDeletionRecord(id: Option[Int],
                                 originalFilePath: String,
                                 objectId: Option[String],
                                 vidispineItemId: Option[String],
                                 mediaTier: MediaTiers.Value,
                                 attempt: Int
                                )

class PendingDeletionRecordRow(tag:Tag) extends Table[PendingDeletionRecord](tag, "mediaremover_pendingdeletion_record") {
  import com.gu.multimedia.storagetier.models.common.MediaTiersEnumMapper._

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def originalFilePath = column[String]("s_original_filepath")
  def objectId = column[Option[String]]("s_object_id")
  def vidispineItemId = column[Option[String]]("s_vidispine_itemid")
  def mediaTier = column[MediaTiers.Value]("s_media_tier")
  def attempt = column[Int]("i_attempt")

  def * = (id.?, originalFilePath, objectId, vidispineItemId, mediaTier, attempt) <> (PendingDeletionRecord.tupled, PendingDeletionRecord.unapply)
}
