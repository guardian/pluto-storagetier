package messages

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor}

import java.time.{Instant, ZoneId, ZonedDateTime}

case class AssetSweeperNewFile(
                              imported_id:Option[String],
                              size:Long,
                              ignore:Boolean,
                              mime_type: String,
                              mtime: ZonedDateTime,
                              ctime: ZonedDateTime,
                              atime: ZonedDateTime,
                              owner: Int,
                              group: Int,
                              filepath: String,
                              filename: String
                              )

object AssetSweeperNewFile {
  object Decoder {
    implicit val decodeAssetSweeperNewFile:Decoder[AssetSweeperNewFile] = new Decoder[AssetSweeperNewFile] {
      final def apply(c: HCursor): Result[AssetSweeperNewFile] = {
        for {
          importedId <- c.downField("imported_id").as[Option[String]]
          size <- c.downField("size").as[Long]
          ignore <- c.downField("ignore").as[Boolean]
          mimeType <- c.downField("mime_type").as[String]
          mTime <- c.downField("mtime").as[Double]
          cTime <- c.downField("ctime").as[Double]
          aTime <- c.downField("atime").as[Double]
          owner <- c.downField("owner").as[Int]
          group <- c.downField("group").as[Int]
          filename <- c.downField("filename").as[String]
          parentDir <- c.downField("parent_dir").as[String]
        } yield AssetSweeperNewFile(
          importedId,
          size,
          ignore,
          mimeType,
          ZonedDateTime.ofInstant(Instant.ofEpochMilli((mTime*1000.0).toLong), ZoneId.of("Etc/UTC")),
          ZonedDateTime.ofInstant(Instant.ofEpochMilli((cTime*1000.0).toLong), ZoneId.of("Etc/UTC")),
          ZonedDateTime.ofInstant(Instant.ofEpochMilli((aTime*1000.0).toLong), ZoneId.of("Etc/UTC")),
          owner,
          group,
          filename,
          parentDir
        )
      }
    }
  }
}