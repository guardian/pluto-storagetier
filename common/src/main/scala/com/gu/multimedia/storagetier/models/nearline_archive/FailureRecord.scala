package com.gu.multimedia.storagetier.models.nearline_archive

import com.gu.multimedia.storagetier.models.common.{ErrorComponents, RetryStates}
import slick.jdbc.PostgresProfile.api._

case class FailureRecord(id:Option[Int],
                         originalFilePath:String,
                         attempt:Int,
                         errorMessage:String,
                         errorComponent:ErrorComponents.Value,
                         retryState:RetryStates.Value
                        )

class FailureRecordRow(tag:Tag) extends Table[FailureRecord](tag, "nearlinearchive_failure_record") {
  import com.gu.multimedia.storagetier.models.common.FailureEnumMapper._

  def id=column[Int]("id", O.PrimaryKey, O.AutoInc)
  def originalFilePath = column[String]("s_original_file_path")
  def attempt = column[Int]("i_attempt")
  def errorMessage = column[String]("s_error_message")
  def errorComponent = column[ErrorComponents.Value]("s_error_component")
  def retryState = column[RetryStates.Value]("s_retry_state")

  def * = (id.?, originalFilePath, attempt, errorMessage, errorComponent, retryState) <> (FailureRecord.tupled, FailureRecord.unapply)
}

