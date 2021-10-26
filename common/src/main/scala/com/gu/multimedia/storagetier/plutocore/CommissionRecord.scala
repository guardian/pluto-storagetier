package com.gu.multimedia.storagetier.plutocore

import java.time.ZonedDateTime

case class CommissionRecord(id:Option[Int],
                            collectionId:Option[Int],
                            siteId: Option[String],
                            created: ZonedDateTime,
                            updated:ZonedDateTime,
                            title: String,
                            status: EntryStatus.Value,
                            description: Option[String],
                            workingGroupId: Int,
                            originalCommissionerName:Option[String],
                            scheduledCompletion:ZonedDateTime,
                            owner:String,
                            notes:Option[String],
                            productionOffice:ProductionOffice.Value,
                            originalTitle:Option[String],
                            googleFolder:Option[String])
