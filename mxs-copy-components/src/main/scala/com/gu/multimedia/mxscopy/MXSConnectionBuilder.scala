package com.gu.multimedia.mxscopy

import com.om.mxs.client.japi.cred.Credentials
import com.om.mxs.client.japi.{MatrixStore, MatrixStoreConnection, Vault}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class MXSConnectionBuilder(hosts: Array[String], clusterId:String, accessKeyId:String, accessKeySecret:String) {
  def build() = Try {
    val credentials = Credentials.newAccessKeyCredentials(accessKeyId, accessKeySecret)

    val conn = MatrixStoreConnection.builder().withHosts(hosts).withClusterId(clusterId).build()
    MatrixStore.builder()
      .withConnection(conn)
      .withCredentials(credentials)
      .build()
  }

  /**
   * initiates a connection to the configuration indicated by the builder, opens the given vault then runs the callback.
   * The callback is expected to return a Future of some type T.
   * Whether it succeeds or fails, the vault connection is then disposed.
   * Use this in preference to the static method if you have no pre-existing connection
   * @param vaultId vault ID to open
   * @param cb callback function which takes the Vault instance as a parameter and returns a Future of any type
   * @param ec implicitly defined execution context
   * @tparam T the type returned by the callback's Future
   * @return either the result of the callback, or a failed Try indicating some error in establishing the connection
   */
  def withVaultFuture[T](vaultId:String)(cb: Vault => Future[T])(implicit ec:ExecutionContext) = {
    Future.fromTry(build()).flatMap(mxs=>{
      MXSConnectionBuilder
        .withVaultFuture(mxs, vaultId)(cb)
        .andThen(_=>mxs.dispose())
    })
  }
}

object MXSConnectionBuilder {
  private val logger = LoggerFactory.getLogger(getClass)

  def withVault[T](mxs: MatrixStore, vaultId: String)(cb: (Vault) => Try[T]) = {
    Try {
      mxs.openVault(vaultId)
    } match {
      case Success(vault) =>
        val result = cb(vault)
        vault.dispose()
        result
      case Failure(err) =>
        logger.error(s"Could not establish vault connection: ${err.getMessage}", err)
        Failure(err)
    }
  }

  def withVaultFuture[T](mxs:MatrixStore, vaultId: String)(cb: (Vault) => Future[T])(implicit ec:ExecutionContext) = {
    Try {
      mxs.openVault(vaultId)
    } match {
      case Success(vault) =>
        cb(vault).andThen(_=>vault.dispose())
      case Failure(err) =>
        logger.error(s"Could not establish vault connection: ${err.getMessage}", err)
        Future.failed(err)
    }
  }
}

