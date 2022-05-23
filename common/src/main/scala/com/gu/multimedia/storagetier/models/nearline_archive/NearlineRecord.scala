package com.gu.multimedia.storagetier.models.nearline_archive

import org.slf4j.MDC
import slick.jdbc.PostgresProfile.api._

case class NearlineRecord(id:Option[Int],
                          objectId: String,
                          originalFilePath:String,
                          vidispineItemId:Option[String],
                          vidispineVersionId:Option[Int],
                          proxyObjectId:Option[String],
                          metadataXMLObjectId:Option[String],
                          internallyArchived:Option[Boolean]=None,
                          expectingVidispineId:Boolean=true,
                          correlationId:String
                         )


object NearlineRecord extends ((Option[Int], String, String, Option[String], Option[Int], Option[String], Option[String],
  Option[Boolean], Boolean, String) =>
  NearlineRecord ) {
  def apply(objectId:String, originalFilePath:String, correlationId: String) = {
    MDC.put("correlationId", correlationId)
    new NearlineRecord(None, objectId, originalFilePath, None, None, None, None, None, false, correlationId)
  }
}

class NearlineRecordRow (tag:Tag) extends Table[NearlineRecord](tag, "nearlinearchive_archived_record") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def objectId = column[String]("s_object_id")
  def originalFilePath = column[String]("s_original_filepath")
  def vidispineItemId = column[Option[String]]("s_vidispine_itemid")
  def vidispineVersionId = column[Option[Int]]("i_vidispine_versionid")
  def proxyObjectId = column[Option[String]]("s_proxy_objectid")
  def metadataXMLObjectId = column[Option[String]]("s_metadata_xml_objectid")
  def internallyArchived = column[Option[Boolean]]("b_internally_archived")
  def expectingVidispineId = column[Boolean]("b_expecting_vidispine_id")
  def correlationId = column[Option[String]]("s_correlation_id")

  def * = (id.?, objectId, originalFilePath, vidispineItemId, vidispineVersionId, proxyObjectId, metadataXMLObjectId, internallyArchived, expectingVidispineId, correlationId.getOrElse("<absent>")) <> (NearlineRecord.tupled, NearlineRecord.unapply)
}
