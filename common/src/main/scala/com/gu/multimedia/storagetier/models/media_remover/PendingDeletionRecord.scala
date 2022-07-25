package com.gu.multimedia.storagetier.models.media_remover

import com.gu.multimedia.storagetier.models.common.MediaTiers
import slick.jdbc.PostgresProfile.api._

case class PendingDeletionRecord(id : Option[Int],
                                 mediaTier : MediaTiers.Value,
                                 originalFilePath : Option[String],
                                 onlineId : Option[String],
                                 nearlineId : Option[String],
                                 attempt : Int
                                )

class PendingDeletionRecordRow(tag:Tag) extends Table[PendingDeletionRecord](tag, "mediaremover_pendingdeletion_record") {
  import com.gu.multimedia.storagetier.models.common.MediaTiersEnumMapper._

  def id=column[Int]("id", O.PrimaryKey, O.AutoInc)
  def mediaTier = column[MediaTiers.Value]("s_media_tier")
  def originalFilePath = column[Option[String]]("s_original_file_path")
  def onlineId = column[Option[String]]("s_online_id")
  def nearlineId = column[Option[String]]("s_nearline_id")
  def attempt = column[Int]("i_attempt")

  def * = (id.?, mediaTier, originalFilePath, onlineId, nearlineId, attempt) <> (PendingDeletionRecord.tupled, PendingDeletionRecord.unapply)
}
