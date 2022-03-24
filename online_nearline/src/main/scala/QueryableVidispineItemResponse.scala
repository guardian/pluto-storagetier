import com.gu.multimedia.storagetier.vidispine.{ItemResponseSimplified, QueryableItem, ShapeDocument}

import scala.util.Try

/**
 * Presents an interface to query an existing item as if it is a recent import
 * @param item
 * @param originalShape
 */
case class QueryableVidispineItemResponse(item:ItemResponseSimplified, originalShape:ShapeDocument) extends QueryableItem {
  override def filePath: Option[String] = item.valuesForField("original_filename", Some("Asset")).headOption.map(_.value)

  override def itemId: Option[String] = item.valuesForField("itemId").headOption.map(_.value)

  override def fileSize: Option[Long] = originalShape.getLikelyFile.map(_.size)

  override def status: Option[String] = None

  override def essenceVersion: Option[Int] = item.valuesForField("__version").headOption.flatMap(v=>Try { v.value.toInt }.toOption)

  override def sourceFileId: Option[String] = originalShape.getLikelyFile.map(_.id)

  override def sourceFileIds: Array[String] = originalShape.getLikelyFile.map(f=>Array(f.id)).getOrElse(Array())

  override def shapeId: Option[String] = Some(originalShape.id)

  override def shapeTag: Option[String] = originalShape.tag.headOption

  override def importSource: Option[String] = Some("existing")
}
