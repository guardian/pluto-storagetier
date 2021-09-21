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

case class ProjectRecord(id: Option[Int], projectTypeId: Int, vidispineProjectId: Option[String],
                         projectTitle: String, created:ZonedDateTime, updated:ZonedDateTime, user: String, workingGroupId: Option[Int],
                         commissionId: Option[Int], deletable: Option[Boolean], deep_archive: Option[Boolean],
                         sensitive: Option[Boolean], status:EntryStatus.Value, productionOffice: ProductionOffice.Value)
