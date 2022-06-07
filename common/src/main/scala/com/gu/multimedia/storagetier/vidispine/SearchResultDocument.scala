package com.gu.multimedia.storagetier.vidispine

case class Item(
    metadata: ItemMetadataSimplified
)

case class SearchResultEntry(
    item: Item,
    `type`: String,
    id: String
)

case class SearchResultDocument(
    hits: Int,
    entry: List[SearchResultEntry]
)
