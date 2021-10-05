package messages

import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._

class VidispineMediaIngestedSpec extends Specification {
  "VidispineMediaIngested" should {
    "get correct data from List of key value pairs from Vidispine" in {
      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-456"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-456=the/correct/filepath/video.mp4")
      ))

      mediaIngested.itemId mustEqual(Some("VX-123"))
      mediaIngested.fileSize mustEqual(Some(100))
      mediaIngested.filePath mustEqual(Some("the/correct/filepath/video.mp4"))
      mediaIngested.status mustEqual(Some("FINISHED"))
    }

    "filePath should return None when matching filePathMap is missing" in {
      val mediaIngested = VidispineMediaIngested(List(
        VidispineField("itemId", "VX-123"),
        VidispineField("bytesWritten", "100"),
        VidispineField("status", "FINISHED"),
        VidispineField("sourceFileId", "VX-NOT_TO_BE_FOUND"),
        VidispineField("filePathMap", "VX-999=some/unknown/path/bla.jpg,VX-1000=the/filepath/video.mp4")
      ))

      mediaIngested.filePath mustEqual(None)
    }
  }
}
