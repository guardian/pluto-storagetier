package com.gu.multimedia.storagetier.models.media_remover

import com.gu.multimedia.storagetier.models.common.MediaTiers
import org.specs2.mutable.Specification
import org.specs2.specification.{AfterEach, BeforeAll}
import slick.jdbc.JdbcBackend.Database
import slick.lifted.TableQuery
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Try

class PendingDeletionRecordDAOSpec extends Specification with BeforeAll with AfterEach {
  sequential

  private lazy val dbHost = sys.env.getOrElse("DB_HOST", "localhost")
  private lazy val dbPort = sys.env.getOrElse("DB_PORT", "5432").toInt
  private lazy val dbUser = sys.env.getOrElse("POSTGRES_USER", "storagetier")
  private lazy val dbPasswd = sys.env.getOrElse("POSTGRES_PASSWORD", "storagetier-test")
  private lazy val dbName = sys.env.getOrElse("POSTGRES_DB", "storagetier-test")
  private val db = Database.forURL(s"jdbc:postgresql://$dbHost:$dbPort/$dbName", user = dbUser, password = dbPasswd)

  private val dao = new PendingDeletionRecordDAO(db)

  override def beforeAll(): Unit = {
    Await.ready(dao.initialiseSchema, 2.seconds)
  }

  override protected def after: Any = {
    Await.ready(
      db.run(
        DBIO.seq(
          TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-id").delete,
          TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-nonexistent-id").delete,
          TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-id-to_delete").delete,
          TableQuery[PendingDeletionRecordRow].filter(_.onlineId==="online-test-id").delete,
          TableQuery[PendingDeletionRecordRow].filter(_.onlineId==="online-test-nonexistent-id").delete,
          TableQuery[PendingDeletionRecordRow].filter(_.onlineId==="online-test-id-to_delete").delete,
        )
      ), 2.seconds
    )
  }

  "PendingDeletionRecordDAO.writeRecord" should {
    "insert a new record, and update it" in {
      //insert a new record, make sure we got something that looks like an id
      val rec = PendingDeletionRecord(None, MediaTiers.NEARLINE, "", Some("vsid"), Some("nearline-test-id"), 1)

      val result = Await.result(dao.writeRecord(rec), 2.seconds)
      result must beGreaterThanOrEqualTo(1)

      //query the number we just got to make sure it is a valid id that points to the record we just wrote
      val updatedRec = rec.copy(id=Some(result))
      val checkRecords = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.id===result).result), 2.seconds)
      checkRecords.length mustEqual 1
      updatedRec mustEqual checkRecords.head

      //make sure we have one record in the table
      val recordCount = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-id").length.result), 2.seconds)
      recordCount mustEqual 1

      //write an update
      val updateResult = Await.result(dao.writeRecord(updatedRec), 2.seconds)
      updateResult mustEqual result

      //make sure we still have one record in the table
      val updatedRecordCount = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-id").length.result), 2.seconds)
      updatedRecordCount mustEqual 1
    }

    "fail if we try to update a record that does not exist" in {
      val rec = PendingDeletionRecord(Some(123), MediaTiers.NEARLINE, "", Some("vsid"), Some("nearline-test-nonexistent-id"), 1)

      val result = Try { Await.result(dao.writeRecord(rec), 2.seconds) }
      result must beAFailedTry
      result.failed.get.getMessage mustEqual "No records were updated"
    }
  }

  "NearlineRecordDAO.deleteRecord" should {
    "delete an existing record" in {
      val rec = PendingDeletionRecord(None, MediaTiers.NEARLINE, "", Some("vsid"), Some("nearline-test-id-to-delete"), 1)

      val insertedId = Await.result(db.run(TableQuery[PendingDeletionRecordRow] returning TableQuery[PendingDeletionRecordRow].map(_.id) += rec), 2.seconds)
      val updatedRec = rec.copy(id=Some(insertedId))

      val beforeDeleteCount = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-id-to-delete").length.result),
        2.seconds)
      beforeDeleteCount mustEqual 1

      val result = Await.result(dao.deleteRecord(updatedRec), 2.seconds)
      result mustEqual 1  //1 record deleted

      val afterDeleteCount = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.nearlineId==="nearline-test-id-to-delete").length.result),
        2.seconds)
      afterDeleteCount mustEqual 0
    }
  }

  "NearlineRecordDAO.deleteRecordByID" should {
    "delete an existing record" in {
      val rec = PendingDeletionRecord(None, MediaTiers.NEARLINE, "", Some("vsid"), Some("nearline-test-id-to-delete"), 1)

      val insertedId = Await.result(db.run(TableQuery[PendingDeletionRecordRow] returning TableQuery[PendingDeletionRecordRow].map(_.id) += rec), 2.seconds)

      val beforeDeleteCount = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.nearlineId === "nearline-test-id-to-delete").length.result),
        2.seconds)
      beforeDeleteCount mustEqual 1

      val result = Await.result(dao.deleteById(insertedId), 2.seconds)
      result mustEqual 1 //1 record deleted

      val afterDeleteCount = Await.result(db.run(TableQuery[PendingDeletionRecordRow].filter(_.nearlineId === "nearline-test-id-to-delete").length.result),
        2.seconds)
      afterDeleteCount mustEqual 0
    }
  }
}
