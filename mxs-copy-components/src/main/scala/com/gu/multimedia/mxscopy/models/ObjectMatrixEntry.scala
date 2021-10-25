package com.gu.multimedia.mxscopy.models

import java.time.{Instant, ZoneId, ZonedDateTime}
import akka.stream.Materializer
import com.om.mxs.client.japi.{MXFSFileAttributes, Vault}
import com.gu.multimedia.mxscopy.helpers.MetadataHelper

import scala.concurrent.ExecutionContext
import scala.util.Try

case class ObjectMatrixEntry(oid:String, attributes:Option[MxsMetadata], fileAttribues:Option[FileAttributes]) {
  def getMxsObject(vault:Vault) = vault.getObject(oid)

  def getMetadata(vault:Vault)(implicit mat:Materializer, ec:ExecutionContext) = MetadataHelper
    .getAttributeMetadata(getMxsObject(vault))
    .map(mxsMeta=>
      this.copy(oid, Some(mxsMeta), Some(FileAttributes(MetadataHelper.getMxfsMetadata(getMxsObject(vault)))))
    )

  /**
   * pull filesystem metadata from the apliance
   * @param vault vault to query
   * @return
   */
  def getMxfsMetadata(vault:Vault) = Try {
    this.copy(oid, fileAttribues = Some(FileAttributes(MetadataHelper.getMxfsMetadata(getMxsObject(vault)))))
  }

  def stringAttribute(key:String) = attributes.flatMap(_.stringValues.get(key))
  def intAttribute(key:String) = attributes.flatMap(_.intValues.get(key))
  def longAttribute(key:String) = attributes.flatMap(_.longValues.get(key))
  def timeAttribute(key:String, zoneId:ZoneId=ZoneId.systemDefault()) = attributes
    .flatMap(_.longValues.get(key))
    .map(v=>ZonedDateTime.ofInstant(Instant.ofEpochMilli(v),zoneId))

  def maybeGetPath() = stringAttribute("MXFS_PATH")
  def maybeGetFilename() = stringAttribute("MXFS_FILENAME")

  def maybeGetSize() = fileAttribues match {
    case Some(fileAttributes) =>
      Some(fileAttributes.size)
    case None =>
      longAttribute("DPSP_SIZE") match {
        case result@Some(_) => result
        case None =>
          stringAttribute("DPSP_SIZE").flatMap(_.toLongOption)
      }
  }

  def pathOrFilename = maybeGetPath() match {
    case Some(p)=>Some(p)
    case None=>maybeGetFilename()
  }
}