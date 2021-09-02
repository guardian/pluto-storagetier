package com.gu.multimedia.storagetier.models

import scala.concurrent.Future

trait GenericDAO[T] {
  /**
   * Write the given record to the database, returning a Future containing the written record primary key.
   * If the `id` field is set then it will attempt to update a record with that primary key, and fail if it can't find one.
   * If the `id` field is not set then it will attempt to write a new record
   * @param rec record to write
   * @return a Future containing the primary key of the written record. If an error occurred, then the future fails;
   *         pick this up using `.recover()` or `.onComplete()`
   */
  def writeRecord(rec:T):Future[Int]

  def deleteRecord(rec:T):Future[Int]

  def deleteById(pk:Int):Future[Int]
}
