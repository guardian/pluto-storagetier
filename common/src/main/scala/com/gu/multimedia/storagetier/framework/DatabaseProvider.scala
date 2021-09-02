package com.gu.multimedia.storagetier.framework

import slick.jdbc.JdbcBackend.Database

object DatabaseProvider {
  /**
   * Get a database instance based on environment variable config.
   * Throws an exception if it fails
   * @return the DatabaseDef, which can be used to make Slick calls
   */
  def get() = {
    lazy val dbHost = sys.env.getOrElse("DB_HOST", "localhost")
    lazy val dbPort = sys.env.getOrElse("DB_PORT", "5432").toInt
    lazy val dbUser = sys.env.getOrElse("POSTGRES_USER", "storagetier")
    lazy val dbPasswd = sys.env.getOrElse("POSTGRES_PASSWORD", "storagetier-test")
    lazy val dbName = sys.env.getOrElse("POSTGRES_DB", "storagetier-test")
    Database.forURL(s"jdbc:postgresql://${dbHost}:${dbPort}/${dbName}",user=dbUser, password=dbPasswd)
  }
}
