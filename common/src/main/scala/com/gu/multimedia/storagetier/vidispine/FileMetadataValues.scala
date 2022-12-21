package com.gu.multimedia.storagetier.vidispine

case class FileMetadataFieldKeyValue(key:String, value:String)
case class FileMetadata(field:Option[Seq[FileMetadataFieldKeyValue]])
