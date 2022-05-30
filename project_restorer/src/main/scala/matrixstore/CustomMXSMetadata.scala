package matrixstore

case class CustomMXSMetadata(itemType:String,
                             projectId:Option[String],
                             commissionId:Option[String],
                             masterId:Option[String],
                             masterName:Option[String],
                             masterUser:Option[String],
                             projectName:Option[String],
                             commissionName:Option[String],
                             workingGroupName:Option[String],
                             deliverableAssetId:Option[Int],
                             deliverableBundle:Option[Int],
                             deliverableVersion:Option[Int],
                             deliverableType:Option[String],
                             vidispineItemId:Option[String]=None,
                             proxyOID:Option[String]=None,      //expect this to be set on original media, if a proxyy exists
                             thumbnailOID:Option[String]=None,  //expect this to be set on original media, if a thumbnail exists
                              metaOID:Option[String]=None,      //expect this to be set on original media, if a VS metadata doc exists
                             mainMediaOID:Option[String]=None,  //expect this to be set on a proxy or thumbnail
                             hidden:Boolean=false) {
  /**
   * adds the contents of the record to the given MxsMetadata object, ignoring empty fields
   * @param addTo existing [[MxsMetadata]] object to add to; this can be `MxsMetadata.empty`
   * @return a new, updated [[MxsMetadata]] object
   */
  def toAttributes(addTo:MxsMetadata):MxsMetadata = {
    val content = Seq(
      projectId.map(s=>"GNM_PROJECT_ID"->s),
      commissionId.map(s=>"GNM_COMMISSION_ID"->s),
      masterId.map(s=>"GNM_MASTER_ID"->s),
      masterName.map(s=>"GNM_MASTER_NAME"->s),
      masterUser.map(s=>"GNM_MASTER_USER"->s),
      projectName.map(s=>"GNM_PROJECT_NAME"->s),
      commissionName.map(s=>"GNM_COMMISSION_NAME"->s),
      workingGroupName.map(s=>"GNM_WORKING_GROUP_NAME"->s),
      deliverableType.map(s=>"GNM_DELIVERABLE_TYPE"->s),
      vidispineItemId.map(s=>"GNM_VIDISPINE_ITEM"->s),
      proxyOID.map(s=>"ATT_PROXY_OID"->s),
      thumbnailOID.map(s=>"ATT_THUMB_OID"->s) ,
      metaOID.map(s=>"ATT_META_OID"->s),
      mainMediaOID.map(s=>"ATT_ORIGINAL_OID"->s) ,
    ).collect({case Some(kv)=>kv}).toMap

    val firstUpdate = content.foldLeft(addTo)((acc,kv)=>acc.withString(kv._1,kv._2))

    val intContent = Seq(
      deliverableAssetId.map(i=>"GNM_DELIVERABLE_ASSETID"->i),
      deliverableBundle.map(i=>"GNM_DELIVERABLE_BUNDLEID"->i),
      deliverableVersion.map(i=>"GNM_DELIVERABLE_VERSION"->i)
    ).collect({case Some(kv)=>kv}).toMap

    val secondUpdate = intContent.foldLeft(firstUpdate)((acc,kv)=>acc.withValue(kv._1,kv._2))

    secondUpdate
      .withValue("GNM_HIDDEN_FILE", hidden)
      .withValue("GNM_TYPE", itemType)
  }
}

object CustomMXSMetadata {
  val TYPE_RUSHES = "rushes"
  val TYPE_DELIVERABLE = "deliverables"
  val TYPE_PROJECT = "project"
  val TYPE_UNSORTED = "unsorted"
  val TYPE_PROXY = "proxy"
  val TYPE_THUMB = "thumb"
  val TYPE_META = "metadata"

  def fromMxsMetadata(incoming:MxsMetadata):Option[CustomMXSMetadata] =
    incoming.stringValues.get("GNM_TYPE").map(itemType=>
      new CustomMXSMetadata(itemType,
        incoming.stringValues.get("GNM_PROJECT_ID"),
        incoming.stringValues.get("GNM_COMMISSION_ID"),
        incoming.stringValues.get("GNM_MASTER_ID"),
        incoming.stringValues.get("GNM_MASTER_NAME"),
        incoming.stringValues.get("GNM_MASTER_USER"),
        incoming.stringValues.get("GNM_PROJECT_NAME"),
        incoming.stringValues.get("GNM_COMMISSION_NAME"),
        incoming.stringValues.get("GNM_WORKING_GROUP_NAME"),
        incoming.intValues.get("GNM_DELIVERABLE_ASSET_ID"),
        incoming.intValues.get("GNM_DELIVERABLE_BUNDLE_ID"),
        incoming.intValues.get("GNM_DELIVERABLE_VERSION"),
        incoming.stringValues.get("GNM_DELIVERABLE_TYPE"),
        incoming.stringValues.get("GNM_VIDISPINE_ITEM"),
        incoming.stringValues.get("ATT_PROXY_OID"),
        incoming.stringValues.get("ATT_THUMB_OID"),
        incoming.stringValues.get("ATT_META_OID"),
        incoming.stringValues.get("ATT_ORIGINAL_OID"),
        incoming.boolValues.getOrElse("GNM_HIDDEN_FILE", false)
      )
    )
}