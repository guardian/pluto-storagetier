package messages

import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._

import java.time.{ZoneId, ZonedDateTime}

class AssetSweeperNewFileSpec extends Specification {
  "AssetSweeperNewFile.Decoder" should {
    "decode a sample message from asset sweeper" in {
      import AssetSweeperNewFile.Decoder._

      val sampleData = """{
                         |  "imported_id": null,
                         |  "size": 110828916,
                         |  "ignore": false,
                         |  "mime_type": "video/mp4",
                         |  "mtime": 1632769744,
                         |  "ctime": 1632904349.6474323,
                         |  "atime": 1632904349.7684443,
                         |  "owner": 501,
                         |  "group": 423452,
                         |  "filename": "zoom_0.mp4",
                         |  "parent_dir": "/media/assets/wg/comm/proj/interviews"
                         |}
                         |""".stripMargin
      io.circe.parser
        .parse(sampleData)
        .flatMap(_.as[AssetSweeperNewFile]) must beRight(AssetSweeperNewFile(
        None,
        110828916,
        false,
        "video/mp4",
        ZonedDateTime.of(2021,9,27,19,9,4,0, ZoneId.of("Etc/UTC")),
        ZonedDateTime.of(2021,9,29,8,32,29,647000000, ZoneId.of("Etc/UTC")),
        ZonedDateTime.of(2021,9,29,8,32,29,768000000, ZoneId.of("Etc/UTC")),
        501,
        423452,
        "/media/assets/wg/comm/proj/interviews",
        "zoom_0.mp4"
      ))
    }
  }
}
