package com.gu.multimedia.storagetier.models

import slick.jdbc.JdbcBackend.Database
import slick.lifted.{AbstractTable, TableQuery}
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Future

/**
 * This trait defines a protocol for our basic data access objects, i.e. to write and delete records
 * @tparam T the type of the database table row class in question
 */
trait GenericDAO[T <: AbstractTable[_]] {
  protected val db:Database
  /**
   * Write the given record to the database, returning a Future containing the written record primary key.
   * If the `id` field is set then it will attempt to update a record with that primary key, and fail if it can't find one.
   * If the `id` field is not set then it will attempt to write a new record
   * @param rec record to write
   * @return a Future containing the primary key of the written record. If an error occurred, then the future fails;
   *         pick this up using `.recover()` or `.onComplete()`
   */
  def writeRecord(rec:T#TableElementType):Future[Int]

  def deleteRecord(rec:T#TableElementType):Future[Int]

  def deleteById(pk:Int):Future[Int]

  /**
   * intialises the given table, if it does not exist yet
   * @return
   */
  def initialiseSchema:Future[Unit]
//  def listRecords(limit:Int, startAt:Int):Future[Seq[T#TableElementType]] = {
//    db.run(
//      TableQuery[T].take(limit).drop(startAt).result
//    )
//  }

}
