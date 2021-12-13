package com.gu.multimedia.mxscopy
import com.om.mxs.client.japi.{MatrixStore, Vault}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

/**
 * Mock implementation of MXSConnectionBuilder that allows simple injection of a mocked vault reference into tests
 * @param mockedVault Vault to inject, normally this would be build with Mockito.
 */
case class MXSConnectionBuilderMock(mockedVault:Vault) extends MXSConnectionBuilder {
  override def getConnection(): Try[MatrixStore] = Failure(new RuntimeException("Build() is not implemented in the mock"))

  override def withVaultFuture[T](vaultId: String)(cb: Vault => Future[Either[String, T]])(implicit ec: ExecutionContext): Future[Either[String, T]] = {
    cb(mockedVault)
  }
}
