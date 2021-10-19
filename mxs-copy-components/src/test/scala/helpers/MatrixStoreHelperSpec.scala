package helpers

import org.specs2.mutable.Specification

class MatrixStoreHelperSpec extends Specification {
  "MatrixStoreHelper.escapeForQuery" should {
    "precede special characters with a single \\" in {
      MatrixStoreHelper.escapeForQuery("Hello {world} & dog") mustEqual "Hello \\{world\\} \\& dog"
      MatrixStoreHelper.escapeForQuery("Hello [[world]] + dog") mustEqual "Hello \\[\\[world\\]\\] \\+ dog"

    }
  }
}
