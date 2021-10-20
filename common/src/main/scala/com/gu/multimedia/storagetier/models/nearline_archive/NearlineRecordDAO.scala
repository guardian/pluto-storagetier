package com.gu.multimedia.storagetier.models.nearline_archive

import com.gu.multimedia.storagetier.models.GenericDAO
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.meta.MTable
import slick.lifted.TableQuery

import scala.concurrent.{ExecutionContext, Future}

class NearlineRecordDAO(override protected val db:Database)(implicit ec:ExecutionContext) extends GenericDAO[NearlineRecordRow]{
  override def writeRecord(rec: NearlineRecord):Future[Int] = {
    rec.id match {
      case None=>               //no id => try to insert the record
        db.run (
          TableQuery[NearlineRecordRow] returning TableQuery[NearlineRecordRow].map(_.id) += rec
        )
      case Some(existingId)=>   //existing id => try to update the record
        db.run(
          TableQuery[NearlineRecordRow].filter(_.id===existingId).update(rec)
        ).flatMap(recordCount=>{
          if(recordCount==0) {
            Future.failed(new RuntimeException("No records were updated"))
          } else {
            Future(existingId)
          }
        })
    }
  }

  override def deleteRecord(rec: NearlineRecord): Future[Int] = {
    db.run(
      TableQuery[NearlineRecordRow].filter(_.id===rec.id).delete
    )
  }

  override def deleteById(pk: Int): Future[Int] = {
    db.run(
      TableQuery[NearlineRecordRow].filter(_.id===pk).delete
    )
  }

  override def initialiseSchema = db.run(
    //Workaround for Slick _always_ trying to create indexes even if .createIfNotExist is used
    //See https://github.com/lagom/lagom/issues/1720#issuecomment-459351282
    MTable.getTables.flatMap { tables=>
      if(!tables.exists(_.name.name == TableQuery[NearlineRecordRow].baseTableRow.tableName)) {
        TableQuery[NearlineRecordRow].schema.create
      } else {
        DBIO.successful(())
      }
    }.transactionally
  )

  override def getRecord(pk: Int): Future[Option[NearlineRecord]] = db.run(
    TableQuery[NearlineRecordRow].filter(_.id===pk).result
  ).map(_.headOption)

  def findBySourceFilename(filename:String) = db.run(
    TableQuery[NearlineRecordRow].filter(_.originalFilePath===filename).result
  ).map(_.headOption)

  def findByVidispineId(vsid:String) = db.run(
    TableQuery[NearlineRecordRow].filter(_.vidispineItemId===vsid).result
  ).map(_.headOption)

}
