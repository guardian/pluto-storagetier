package com.gu.multimedia.storagetier.models.online_archive

import slick.jdbc.PostgresProfile.api._

object ErrorComponents extends Enumeration {
  val Internal, Vidispine, AWS = Value
}

object RetryStates extends  Enumeration {
  val WillRetry, RanOutOfRetries = Value
}

object FailureEnumMapper {
  implicit val errorComponentsMapper = MappedColumnType.base[ErrorComponents.Value, String](
    e=>e.toString,
    s=>ErrorComponents.withName(s)
  )
  implicit val retryStatesMapper = MappedColumnType.base[RetryStates.Value, String](
    e=>e.toString,
    s=>RetryStates.withName(s)
  )
}

case class FailureRecord(id:Option[Int],
                         originalFilePath:String,
                         attempt:Int,
                         errorMessage:String,
                         errorComponent:ErrorComponents.Value,
                         retryState:RetryStates.Value
                        )

class FailureRecordRow(tag:Tag) extends Table[FailureRecord](tag, "onlinearchive_failure_record") {
  import FailureEnumMapper._

  def id=column[Int]("id", O.PrimaryKey, O.AutoInc)
  def originalFilePath = column[String]("s_original_file_path")
  def attempt = column[Int]("i_attempt")
  def errorMessage = column[String]("s_error_message")
  def errorComponent = column[ErrorComponents.Value]("s_error_component")
  def retryState = column[RetryStates.Value]("s_retry_state")

  def * = (id.?, originalFilePath, attempt, errorMessage, errorComponent, retryState) <> (FailureRecord.tupled, FailureRecord.unapply)
}

