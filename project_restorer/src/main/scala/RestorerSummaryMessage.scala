import java.time.ZonedDateTime

case class RestorerSummaryMessage(projectId: Int,
                                  completed: ZonedDateTime,
                                  projectState: String,
                                  numberOfAssociatedFilesNearline: Int,
                                  numberOfAssociatedFilesOnline: Int)