package plutocore

import io.circe.{Decoder, Encoder}

import java.sql.Timestamp
import java.time.ZonedDateTime

object EntryStatus extends Enumeration {
  val New,Held,Completed,Killed = Value
  val InProduction = Value("In Production")
}

object ProductionOffice extends Enumeration {
  val UK,US,Aus = Value
}

object ProjectRecordEncoder {
  implicit val statusDecoder:Decoder[EntryStatus.Value] = Decoder.decodeEnumeration(EntryStatus)
  implicit val statusEncoder:Encoder[EntryStatus.Value] = Encoder.encodeEnumeration(EntryStatus)
  implicit val prodOfficeDecoder:Decoder[ProductionOffice.Value] = Decoder.decodeEnumeration(ProductionOffice)
  implicit val prodOfficeEncoder:Encoder[ProductionOffice.Value] = Encoder.encodeEnumeration(ProductionOffice)
}
/*
{"status":"ok","result":{"id":1,
  "projectTypeId":1,
  "title":"First test project",
  "created":"2021-08-31T13:36:05.58Z",
  "updated":"2021-08-31T13:36:05.58Z",
  "user":"testuser",
  "workingGroupId":2,
  "commissionId":1,
  "deletable":true,
  "deep_archive":false,
  "sensitive":false,
  "status":"New",
  "productionOffice":"UK"}}
 */
case class ProjectRecord(id: Option[Int], projectTypeId: Int, title: String, created:ZonedDateTime,
                         updated:ZonedDateTime, user: String, workingGroupId: Option[Int],
                         commissionId: Option[Int], deletable: Option[Boolean], deep_archive: Option[Boolean],
                         sensitive: Option[Boolean], status:EntryStatus.Value, productionOffice: ProductionOffice.Value)
