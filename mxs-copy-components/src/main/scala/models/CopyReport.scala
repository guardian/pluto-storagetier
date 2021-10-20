package models

case class CopyReport[T] (filename:String, oid:String, checksum:Option[String], size:Long, preExisting:Boolean, validationPassed:Option[Boolean], extraData:Option[T]=None)