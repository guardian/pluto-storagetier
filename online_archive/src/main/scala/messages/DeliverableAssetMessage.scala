package messages

import java.time.ZonedDateTime

/**
 * message format that is used for `deliverables.deliverableasset.create` and `deliverables.deliverableasset.update`
 * on the pluto-deliverables exchange
 */
case class DeliverableAssetMessage(
                                  id:Int,
                                  deliverableTypeId:Option[Int],
                                  filename:String,
                                  size:Long,
                                  access_dt:Option[ZonedDateTime],
                                  modified_dt:Option[ZonedDateTime],
                                  changed_dt:Option[ZonedDateTime],
                                  job_id:Option[String],
                                  online_item_id:Option[String],
                                  nearline_item_id:Option[String],
                                  archive_item_id:Option[String],
                                  deliverable:Int,
                                  status:Int,
                                  type_string:Option[String],
                                  atom_id:Option[String],
                                  status_string:Option[String],
                                  absolute_path:Option[String],
                                  linked_to_lowres:Option[Boolean]
                                  )

