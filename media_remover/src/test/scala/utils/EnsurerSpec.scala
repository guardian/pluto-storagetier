package utils

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.util.Try

class EnsurerSpec extends Specification with Mockito {
//  "MediaNotRequiredMessageProcessor.storeDeletionPending" should {
  "Utils.validateNeededFields" should {
    "fail when size is None" in {

      val result = Try {
        Ensurer.validateNeededFields(None, Some("path"), Some("id"))
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "fileSize is missing"
    }

    "fail when size is -1" in {

      val result = Try {
        Ensurer.validateNeededFields(Some(-1), Some("path"), Some("id"))
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "fileSize is -1"
    }

    "fail when path is missing" in {

      val result = Try {
        Ensurer.validateNeededFields(Some(2048), None, Some("id"))
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "filePath is missing"
    }

    "fail when id is missing" in {

      val result = Try {
        Ensurer.validateNeededFields(Some(2048), Some("path"), None)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "media id is missing"
    }

    "fail when when size, path and id are all missing" in {

      val result = Try {
        Ensurer.validateNeededFields(None, None, None)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "fileSize is missing"
    }

    "succeed when size, path and id are present" in {


      val result = Try {
        Ensurer.validateNeededFields(Some(2048), Some("path"), Some("id"))
      }

      result must beSuccessfulTry
    }
  }}
