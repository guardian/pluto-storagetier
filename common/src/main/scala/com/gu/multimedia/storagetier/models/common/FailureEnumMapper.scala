package com.gu.multimedia.storagetier.models.common
import slick.jdbc.PostgresProfile.api._

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
