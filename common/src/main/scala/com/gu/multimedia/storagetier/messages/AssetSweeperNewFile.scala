package com.gu.multimedia.storagetier.messages

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

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
  object Codec {
    implicit val encodeAssetSweeperNewFile:Encoder[AssetSweeperNewFile] = new Encoder[AssetSweeperNewFile] {
      override def apply(a: AssetSweeperNewFile): Json = Json.obj(
        ("imported_id", a.imported_id.map(Json.fromString).getOrElse(Json.Null)),
        ("size", Json.fromLong(a.size)),
        ("ignore", Json.fromBoolean(a.ignore)),
        ("mime_type", Json.fromString(a.mime_type)),
        ("mtime", Json.fromLong(a.mtime.toInstant.toEpochMilli/1000)),
        ("ctime", Json.fromLong(a.ctime.toInstant.toEpochMilli/1000)),
        ("atime", Json.fromLong(a.atime.toInstant.toEpochMilli/1000)),
        ("owner", Json.fromInt(a.owner)),
        ("group", Json.fromInt(a.group)),
        ("parent_dir", Json.fromString(a.filepath)),
        ("filename", Json.fromString(a.filename))
      )
    }

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
          parentDir <- c.downField("parent_dir").as[String]
          filename <- c.downField("filename").as[String]
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
          parentDir,
          filename
        )
      }
    }
  }
}