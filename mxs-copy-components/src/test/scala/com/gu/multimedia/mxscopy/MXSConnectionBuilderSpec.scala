package com.gu.multimedia.mxscopy

import com.om.mxs.client.japi.{MatrixStore, Vault}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class MXSConnectionBuilderSpec extends Specification with Mockito {
  "MXSConnectionBuilder.withVault" should {
    "call the provided function and dispose the vault afterwards" in {
      val mockVault = mock[Vault]

      val mxs = mock[MatrixStore]
      mxs.openVault(any) returns mockVault

      val checker = mock[Vault=>Unit]

      MXSConnectionBuilder.withVault(mxs, "some-vault") { v=>
        checker(v)
        Success("Hooray")
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

      MXSConnectionBuilder.withVault(mxs, "some-vault") { v=>
        checker(v)
        Failure(new RuntimeException("boo"))
      }

      there was one(checker).apply(mockVault)
      there was one(mockVault).dispose()
      there was one(mxs).openVault("some-vault")
    }
  }

  "MXSConnectionBuilder.withVaultFuture" should {
    "call the provided function and dispose the vault afterwards" in {
      val mockVault = mock[Vault]

      val mxs = mock[MatrixStore]
      mxs.openVault(any) returns mockVault

      val checker = mock[Vault=>Unit]

      MXSConnectionBuilder.withVaultFuture(mxs, "some-vault") { v=>
        checker(v)
        Future("Hooray")
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

      MXSConnectionBuilder.withVaultFuture(mxs, "some-vault") { v=>
        checker(v)
        Future.failed(new RuntimeException("boo"))
      }

      there was one(checker).apply(mockVault)
      there was one(mockVault).dispose()
      there was one(mxs).openVault("some-vault")
    }
  }
}
