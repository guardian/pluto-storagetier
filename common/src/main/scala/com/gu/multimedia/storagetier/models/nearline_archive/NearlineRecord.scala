package com.gu.multimedia.storagetier.models.nearline_archive

import slick.jdbc.PostgresProfile.api._

case class NearlineRecord(id:Option[Int],
                          objectId: String,
                          originalFilePath:String,
                          vidispineItemId:Option[String],
                          vidispineVersionId:Option[Int],
                          proxyObjectId:Option[String],
                          metadataXMLObjectId:Option[String]
                         )

object NearlineRecord extends ((Option[Int], String, String, Option[String], Option[Int], Option[String], Option[String]) =>
  NearlineRecord ) {
  def apply(objectId:String, originalFilePath:String) = {
    new NearlineRecord(None, objectId, originalFilePath, None, None, None, None)
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

  def * = (id.?, objectId, originalFilePath, vidispineItemId, vidispineVersionId, proxyObjectId, metadataXMLObjectId) <>
    (NearlineRecord.tupled, NearlineRecord.unapply)
}