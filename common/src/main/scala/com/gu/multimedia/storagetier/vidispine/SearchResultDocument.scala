package com.gu.multimedia.storagetier.vidispine

import org.slf4j.LoggerFactory

import scala.util.Try

case class SearchResultDocument(
    hits: Int,
    entry: List[SearchResultItemSimplified]
)
case class SearchResultItemSimplified(
    item: SearchResultItemContentSimplified,
    id: String
) {

  /** Returns the metadata values, as a string, for the given field. By default only "root" level fields are searched
    * but you can look inside a group instead by setting `maybeGroupname`
    * @param fieldName field to look for
    * @param maybeGroupname group name to search within
    * @return a sequence of `MetadataValuesWrite`
    */
  def valuesForField(
      fieldName: String,
      maybeGroupname: Option[String] = None
  ): Seq[MetadataValuesWrite] = {
    val timespans =
      item.metadata.timespan.filter(t => t.start == "-INF" && t.end == "+INF")

    val fieldsToSearch = maybeGroupname match {
      case None => timespans.flatMap(_.field)
      case Some(groupName) =>
        timespans.flatMap(_.group.filter(_.name == groupName).flatMap(_.field))
    }

    fieldsToSearch.filter(_.name == fieldName).flatMap(_.value)
  }
}
case class SearchResultItemContentSimplified(
    metadata: ItemMetadataSimplified,
    shape: Seq[ShapeDocument]
)

case class VSOnlineOutputMessage(mediaTier: String,
                                 projectIds: Seq[Int],
                                 filePath: Option[String],
                                 fileSize: Option[Long],
                                 itemId: Option[String],
                                 nearlineId: Option[String],
                                 mediaCategory: String
)
object VSOnlineOutputMessage {
  private val logger = LoggerFactory.getLogger(getClass)

  def fromResponseItem(
      itemSimplified: SearchResultItemSimplified,
      projectId: Int
  ): Option[VSOnlineOutputMessage] = {
    logger.info(s"itemSimplified: $itemSimplified")
    logger.info(s"itemSimplified.item: ${itemSimplified.item}")

    val mediaTier = "ONLINE"
    val itemId = Option(itemSimplified.id)
    val likelyFile = itemSimplified.item.shape.headOption.flatMap(_.getLikelyFile)
    logger.info(s"likelyFile: $likelyFile")
    val filePath = likelyFile.flatMap(_.getAbsolutePath)
    val fileSize = likelyFile.flatMap(_.sizeOption)
    val projectIdAndContainingProjectIds = projectId +: safeGetContainingProjects(itemSimplified)
    val nearlineId = itemSimplified
      .valuesForField("gnm_nearline_id", Some("Asset"))
      .headOption
      .map(_.value)
    val mediaCategory = itemSimplified
      .valuesForField("gnm_category", Some("Asset"))
      .headOption
      .map(_.value)
    (itemId, mediaCategory) match {
      case (Some(itemId), Some(mediaCategory)) =>
        Some(
          VSOnlineOutputMessage(
            mediaTier,
            projectIdAndContainingProjectIds,
            filePath,
            fileSize,
            Some(itemId),
            nearlineId,
            mediaCategory
          )
        )
      case _ =>
        logger.warn(s"VS response missing itemId ($itemId) and/or mediaCategory ($mediaCategory)")
        None
    }
  }

  private def safeGetContainingProjects(itemSimplified: SearchResultItemSimplified) = {
    itemSimplified.valuesForField("gnm_containing_projects", Some("Asset"))
      .map(_.value)
      .map(v => Try { v.toInt }.toOption)
      .collect({ case Some(i) => i })
  }
}
