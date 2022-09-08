package com.gu.multimedia.storagetier.models.media_remover

import com.gu.multimedia.storagetier.models.GenericDAO
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.common.MediaTiersEnumMapper._
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.meta.MTable
import slick.lifted.TableQuery

import scala.concurrent.{ExecutionContext, Future}

class PendingDeletionRecordDAO(override protected val db: Database)(implicit
    ec: ExecutionContext
) extends GenericDAO[PendingDeletionRecordRow] {

  /** Write the given record to the database, returning a Future containing the written record primary key.
    * If the `id` field is set then it will attempt to update a record with that primary key, and fail if it can't find one.
    * If the `id` field is not set then it will attempt to write a new record
    *
    * @param rec record to write
    * @return a Future containing the primary key of the written record. If an error occurred, then the future fails;
    *         pick this up using `.recover()` or `.onComplete()`
    */
  override def writeRecord(rec: PendingDeletionRecord): Future[Int] =
    rec.id match {
      case None =>
        db.run(
          TableQuery[PendingDeletionRecordRow] returning TableQuery[
            PendingDeletionRecordRow
          ].map(_.id) += rec
        )
      case Some(existingId) =>
        db.run(
          TableQuery[PendingDeletionRecordRow]
            .filter(_.id === existingId)
            .update(rec)
        ).flatMap(rows => {
          if (rows < 1) {
            Future.failed(
              new RuntimeException("No records were updated")
            )
          } else {
            Future(existingId)
          }
        })
    }

  override def deleteRecord(rec: PendingDeletionRecord): Future[Int] =
    rec.id match {
      case None =>
        Future.failed(
          new RuntimeException(
            "You can't delete a record that has no primary key set"
          )
        )
      case Some(existingId) =>
        db.run(
          TableQuery[PendingDeletionRecordRow]
            .filter(_.id === existingId)
            .delete
        )
    }

  override def deleteById(pk: Int): Future[Int] =
    db.run(
      TableQuery[PendingDeletionRecordRow].filter(_.id === pk).delete
    )

  override def getRecord(pk: Int): Future[Option[PendingDeletionRecord]] =
    db.run(
      TableQuery[PendingDeletionRecordRow].filter(_.id === pk).result
    ).map(_.headOption)

  override def initialiseSchema: Future[Unit] =
    db.run(
      //Workaround for Slick _always_ trying to create indexes even if .createIfNotExist is used
      //See https://github.com/lagom/lagom/issues/1720#issuecomment-459351282
      MTable.getTables.flatMap(
        tables => if (!tables.exists(_.name.name == TableQuery[PendingDeletionRecordRow].baseTableRow.tableName)) {
          TableQuery[PendingDeletionRecordRow].schema.create
        } else {
          DBIO.successful(())
        }
      ).transactionally
    )


  /**
   * sets the "number of times we've tried to delete this media" value for the given record, without touching anything else
   *
   * @param pk ID of the record to update
   * @param newAttemptCount new value to set for the attempt
   * @return a Future, containing the number of records updated
   */
  def updateAttemptCount(pk:Int, newAttemptCount:Int) = {
    val q = for {
      row <- TableQuery[PendingDeletionRecordRow] if row.id===pk
    } yield row.attempt

    db.run(
      q.update(newAttemptCount)
    )
  }

  def findBySourceFilenameAndMediaTier(filename:String, mediaTier: MediaTiers.Value): Future[Option[PendingDeletionRecord]] =
    db.run(
      TableQuery[PendingDeletionRecordRow].filter(row => row.originalFilePath===filename && row.mediaTier===mediaTier).result
    ).map(_.headOption)

  def findByOnlineIdForONLINE(vsItemId: String): Future[Option[PendingDeletionRecord]] =
    db.run(
      TableQuery[PendingDeletionRecordRow].filter(
        row => row.vidispineItemId===vsItemId && row.mediaTier===MediaTiers.ONLINE
      ).result
    ).map(_.headOption)

  def findByNearlineIdForNEARLINE(oid: String): Future[Option[PendingDeletionRecord]] =
    db.run(
      TableQuery[PendingDeletionRecordRow].filter(
        row => row.nearlineId===oid && row.mediaTier===MediaTiers.NEARLINE
      ).result
    ).map(_.headOption)

}
