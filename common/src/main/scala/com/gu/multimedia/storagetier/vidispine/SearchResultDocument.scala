package com.gu.multimedia.storagetier.vidispine

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

case class VSOnlineOutputMessage(
    mediaTier: String,
    projectId: Int,
    filePath: Option[String],
    itemId: Option[String],
    nearlineId: String,
    mediaCategory: String
)
object VSOnlineOutputMessage {

  def fromResponseItem(
      itemSimplified: SearchResultItemSimplified,
      projectId: Int
  ): Option[VSOnlineOutputMessage] = {
    val mediaTier = "ONLINE"
    val itemId = Option(itemSimplified.id)
    val filePath =
      itemSimplified.item.shape.head.getLikelyFile.flatMap(_.getAbsolutePath)
    val nearlineId = itemSimplified
      .valuesForField("gnm_nearline_id", Some("Asset"))
      .headOption
      .map(_.value)
    val mediaCategory = itemSimplified
      .valuesForField("gnm_category", Some("Asset"))
      .headOption
      .map(_.value)
    (nearlineId, mediaCategory) match {
      case (Some(nearlineId), Some(mediaCategory)) =>
        Some(
          VSOnlineOutputMessage(
            mediaTier,
            projectId,
            filePath,
            itemId,
            nearlineId,
            mediaCategory
          )
        )
      case _ =>
        None
    }
  }
}
