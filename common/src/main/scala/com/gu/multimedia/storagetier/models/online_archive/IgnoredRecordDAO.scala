package com.gu.multimedia.storagetier.models.online_archive

import com.gu.multimedia.storagetier.models.GenericDAO
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import slick.jdbc.JdbcBackend.Database
import slick.lifted.TableQuery
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.meta.MTable

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class IgnoredRecordDAO(override val db:Database) extends GenericDAO[IgnoredRecordRow] {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Write the given record to the database, returning a Future containing the written record primary key.
   * If the `id` field is set then it will attempt to update a record with that primary key, and fail if it can't find one.
   * If the `id` field is not set then it will attempt to write a new record
   *
   * @param rec record to write
   * @return a Future containing the primary key of the written record. If an error occurred, then the future fails;
   *         pick this up using `.recover()` or `.onComplete()`
   */
  override def writeRecord(rec: IgnoredRecord): Future[Int] = {
    rec.id match {
      case None=>
        db.run(
          TableQuery[IgnoredRecordRow] returning TableQuery[IgnoredRecordRow].map(_.id) += rec
        ).recoverWith({
          case err:PSQLException=>
            //catch duplicate key value errors as we don't want them to be fatal.
            //if you need to find out whether the record was set then check the returned int, 0 => nothing
            if(err.getMessage.contains("duplicate key value violates unique constraint")) {
              logger.debug(s"already have a record for ${rec.originalFilePath}")
              Future(0)
            } else {
              Future.failed(err)
            }
        })
      case Some(existingId)=>
        db.run(
          TableQuery[IgnoredRecordRow].filter(_.id===existingId).update(rec)
        ).flatMap(rows=>{
          if(rows<1) {
            Future.failed(new RuntimeException("No records were updated"))
          } else {
            Future(existingId)
          }
        })
    }
  }

  override def deleteRecord(rec: IgnoredRecord): Future[Int] = {
    rec.id match {
      case None =>
        Future.failed(new RuntimeException("You can't delete a record that has no primary key set"))
      case Some(existingId) =>
        db.run(
          TableQuery[IgnoredRecordRow].filter(_.id === existingId).delete
        )
    }
  }

  override def deleteById(pk: Int): Future[Int] = {
    db.run(
      TableQuery[IgnoredRecordRow].filter(_.id===pk).delete
    )
  }

  override def initialiseSchema = db.run(
    //Workaround for Slick _always_ trying to create indexes even if .createIfNotExist is used
    //See https://github.com/lagom/lagom/issues/1720#issuecomment-459351282
    MTable.getTables.flatMap { tables=>
      if(!tables.exists(_.name.name == TableQuery[IgnoredRecordRow].baseTableRow.tableName)) {
        TableQuery[IgnoredRecordRow].schema.create
      } else {
        DBIO.successful(())
      }
    }.transactionally

  )

  override def getRecord(pk: Int): Future[Option[IgnoredRecord]] = db.run(
    TableQuery[IgnoredRecordRow].filter(_.id===pk).result
  ).map(_.headOption)
}
