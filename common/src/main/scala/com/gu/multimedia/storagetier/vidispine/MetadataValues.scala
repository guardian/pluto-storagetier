package com.gu.multimedia.storagetier.vidispine

/*
data models for constructing metadata updates that go to Vidispine.
These "write" variants contain only the fields you need to _push_ data, not the extra fields that come back
when you read it
 */
case class MetadataValuesWrite(value:String)
case class MetadataFieldWrite(name:String, value:Seq[MetadataValuesWrite])
case class MetadataFieldGroupWrite(name:String, field:Seq[MetadataFieldWrite])
case class Timespan(field:Seq[MetadataFieldWrite],  group:Seq[MetadataFieldGroupWrite], start:String="-INF", end:String="+INF")
case class MetadataWrite(timespan:Seq[Timespan])

object MetadataWrite {
  /**
   * convenience method that builds a serializable object to write a key/value pair to item metadata
   * @param field field name to set
   * @param value value to set
   * @return a MetadataWrite document, that can be serialized with `.asJson.noSpaces`
   */
  def simpleKeyValue(field:String, value:String):MetadataWrite = MetadataWrite(
    Seq(
      Timespan(
        field = Seq(
          MetadataFieldWrite(field, value=Seq(MetadataValuesWrite(value)))
        ),
        group = Seq()
      )
    )
  )

  /**
   * convenience method that builds a serializable object to write a  number of values to the same field in item metadata
   * @param field field name to set
   * @param values values to set
   * @return a MetadataWrite document, that can be serialized with `.asJson.noSpaces`
   */
  def keyMultipleValue(field: String, values:Seq[String]):MetadataWrite = MetadataWrite(
    Seq(
      Timespan(
        field = Seq(
          MetadataFieldWrite(field, value=values.map(MetadataValuesWrite.apply))
        ),
        group = Seq()
      )
    )
  )
}

case class ItemMetadataSimplified(revision:String, timespan:Seq[Timespan])
case class ItemResponseContentSimplified(metadata:Seq[ItemMetadataSimplified])
case class ItemResponseSimplified(item:Seq[ItemResponseSimplified])