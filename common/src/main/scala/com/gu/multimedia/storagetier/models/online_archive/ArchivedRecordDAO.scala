package com.gu.multimedia.storagetier.models.online_archive
import com.gu.multimedia.storagetier.models.GenericDAO
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcBackend.Database
import slick.lifted.TableQuery
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

class ArchivedRecordDAO(override protected val db:Database) extends GenericDAO[ArchivedRecordRow]{
  override def writeRecord(rec: ArchivedRecord):Future[Int] = {
    rec.id match {
      case None=>               //no id => try to insert the record
        db.run (
          TableQuery[ArchivedRecordRow] returning TableQuery[ArchivedRecordRow].map(_.id) += rec
        )
      case Some(existingId)=>   //existing id => try to update the record
        db.run(
          TableQuery[ArchivedRecordRow].filter(_.id===existingId).update(rec)
        ).flatMap(recordCount=>{
          if(recordCount==0) {
            Future.failed(new RuntimeException("No records were updated"))
          } else {
            Future(existingId)
          }
        })
    }
  }

  override def deleteRecord(rec: ArchivedRecord): Future[Int] = {
    db.run(
      TableQuery[ArchivedRecordRow].filter(_.id===rec.id).delete
    )
  }

  override def deleteById(pk: Int): Future[Int] = {
    db.run(
      TableQuery[ArchivedRecordRow].filter(_.id===pk).delete
    )
  }
}
