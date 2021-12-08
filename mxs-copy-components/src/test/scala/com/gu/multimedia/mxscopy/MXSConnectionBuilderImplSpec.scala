package com.gu.multimedia.mxscopy

import com.om.mxs.client.japi.{MatrixStore, Vault}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class MXSConnectionBuilderImplSpec extends Specification with Mockito {
  "MXSConnectionBuilderImpl.withVault" should {
    "call the provided function and dispose the vault afterwards" in {
      val mockVault = mock[Vault]

      val mxs = mock[MatrixStore]
      mxs.openVault(any) returns mockVault

      val checker = mock[Vault=>Unit]

      MXSConnectionBuilderImpl.withVault(mxs, "some-vault") { v=>
        checker(v)
        Success(Right("Hooray"))
      }

      there was one(checker).apply(mockVault)
      there was one(mockVault).dispose()
      there was one(mxs).openVault("some-vault")
    }

    "dispose the vault if the callback fails" in {
      val mockVault = mock[Vault]

      val mxs = mock[MatrixStore]
      mxs.openVault(any) returns mockVault

      val checker = mock[Vault=>Unit]

      MXSConnectionBuilderImpl.withVault(mxs, "some-vault") { v=>
        checker(v)
        Failure(new RuntimeException("boo"))
      }

      there was one(checker).apply(mockVault)
      there was one(mockVault).dispose()
      there was one(mxs).openVault("some-vault")
    }
  }

  "MXSConnectionBuilderImpl.withVaultFuture" should {
    "call the provided function and dispose the vault afterwards" in {
      val mockVault = mock[Vault]

      val mxs = mock[MatrixStore]
      mxs.openVault(any) returns mockVault

      val checker = mock[Vault=>Unit]

      Await.ready(MXSConnectionBuilderImpl.withVaultFuture(mxs, "some-vault") { v=>
        checker(v)
        Future(Right("Hooray"))
      },2.seconds)

      there was one(checker).apply(mockVault)
      there was one(mockVault).dispose()
      there was one(mxs).openVault("some-vault")
    }

    "dispose the vault if the callback fails" in {
      val mockVault = mock[Vault]

      val mxs = mock[MatrixStore]
      mxs.openVault(any) returns mockVault

      val checker = mock[Vault=>Unit]

      Await.ready(MXSConnectionBuilderImpl.withVaultFuture(mxs, "some-vault") { v=>
        checker(v)
        Future.failed(new RuntimeException("boo"))
      }, 2.seconds)

      there was one(checker).apply(mockVault)
      there was one(mockVault).dispose()
      there was one(mxs).openVault("some-vault")
    }
  }
}
