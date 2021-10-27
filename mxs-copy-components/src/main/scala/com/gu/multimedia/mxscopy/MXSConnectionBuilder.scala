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

