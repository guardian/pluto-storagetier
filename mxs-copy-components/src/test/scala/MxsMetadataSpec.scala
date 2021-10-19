import models.MxsMetadata
import org.specs2.mutable.Specification

class MxsMetadataSpec extends Specification {
  "MsxMetadata.merge" should {
    "prefer keys of the merger rather than mergee" in {
      val meta1 = MxsMetadata.empty
        .withString("keyone","valueone")
        .withString("keytwo","valuetwo")

      val meta2 = MxsMetadata.empty
        .withString("keytwo","anothervalue")
        .withString("keythree","valuethree")

      val result = meta1.merge(meta2)

      result.stringValues.get("keyone") must beSome("valueone")
      result.stringValues.get("keytwo") must beSome("anothervalue")
      result.stringValues.get("keythree") must beSome("valuethree")
      result.stringValues.get("keyfour") must beNone

      result mustNotEqual meta1
      result mustNotEqual meta2
    }
  }
}
