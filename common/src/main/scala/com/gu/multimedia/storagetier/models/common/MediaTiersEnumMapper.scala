package com.gu.multimedia.storagetier.models.common

import slick.jdbc.PostgresProfile.api._

object MediaTiersEnumMapper {
  implicit val mediaTiersMapper = MappedColumnType.base[MediaTiers.Value, String](
    e=>e.toString,
    s=>MediaTiers.withName(s)
  )
}
