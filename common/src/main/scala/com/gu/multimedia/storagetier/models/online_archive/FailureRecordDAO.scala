package com.gu.multimedia.storagetier.models.online_archive

import com.gu.multimedia.storagetier.models.GenericDAO
import slick.jdbc.JdbcBackend.Database
import slick.lifted.TableQuery
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class FailureRecordDAO(db:Database) extends GenericDAO[FailureRecord] {
  /**
   * Write the given record to the database, returning a Future containing the written record primary key.
   * If the `id` field is set then it will attempt to update a record with that primary key, and fail if it can't find one.
   * If the `id` field is not set then it will attempt to write a new record
   *
   * @param rec record to write
   * @return a Future containing the primary key of the written record. If an error occurred, then the future fails;
   *         pick this up using `.recover()` or `.onComplete()`
   */
  override def writeRecord(rec: FailureRecord): Future[Int] = {
    rec.id match {
      case None=>
        db.run(
          TableQuery[FailureRecordRow] returning TableQuery[FailureRecordRow].map(_.id) += rec
        )
      case Some(existingId)=>
        db.run(
          TableQuery[FailureRecordRow].filter(_.id===existingId).update(rec)
        ).flatMap(rows=>{
          if(rows<1) {
            Future.failed(new RuntimeException("No records were updated"))
          } else {
            Future(existingId)
          }
        })
    }
  }

  override def deleteRecord(rec: FailureRecord): Future[Int] = {
    rec.id match {
      case None =>
        Future.failed(new RuntimeException("You can't delete a record that has no primary key set"))
      case Some(existingId) =>
        db.run(
          TableQuery[FailureRecordRow].filter(_.id === existingId).delete
        )
    }
  }

  override def deleteById(pk: Int): Future[Int] = {
    db.run(
      TableQuery[FailureRecordRow].filter(_.id === pk).delete
    )
  }
}
