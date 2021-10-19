import helpers.Copier
import org.specs2.mutable.Specification

class CopierSpec extends Specification {
  "removeLeadingSlash" should {
    "remove the leading slash from a filepath" in {
      val result = Copier.removeLeadingSlash("/path/to/something")
      result mustEqual "path/to/something"
    }

    "must pass through a path without leading slash with no change" in {
      val result = Copier.removeLeadingSlash("path/to/something")
      result mustEqual("path/to/something")
    }
  }
}
