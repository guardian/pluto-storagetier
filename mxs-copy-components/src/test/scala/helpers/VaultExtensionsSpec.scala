package helpers

import com.om.mxs.client.japi.{MxsObject, Vault}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import testhelpers.AkkaTestkitSpecs2Support

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class VaultExtensionsSpec extends Specification with Mockito {
  "VaultExtensions.getObjectWithTimeout" should {
    "return the object in a Future if it succeeds" in new AkkaTestkitSpecs2Support {
      import VaultExtensions._
      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val result = Await.result(mockVault.getObjectWithTimeout("some-fake-id", 2.seconds), 2.seconds)
      result mustEqual mockObject
      there was one(mockVault).getObject("some-fake-id")
    }

    "timeout if the read takes longer than the timeout" in new AkkaTestkitSpecs2Support {
      import VaultExtensions._

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) answers  ( (_:Any)=>{
        Thread.sleep(3000)
        mockObject
      })

      val result = Try {
        Await.result(mockVault.getObjectWithTimeout("some-fake-id", 2.seconds), 5.seconds)
      }
      result must beFailedTry
      result.failed.get.getMessage mustEqual "getObject timed out"
    }

    "pass on any exception that occurs in the underlying sdk" in new AkkaTestkitSpecs2Support {
      import VaultExtensions._

      val mockVault = mock[Vault]
      mockVault.getObject(any) throws new RuntimeException("kaboom")

      val result = Try {
        Await.result(mockVault.getObjectWithTimeout("some-fake-id", 2.seconds), 5.seconds)
      }
      result must beFailedTry
      result.failed.get.getMessage mustEqual "kaboom"
    }
  }

}
