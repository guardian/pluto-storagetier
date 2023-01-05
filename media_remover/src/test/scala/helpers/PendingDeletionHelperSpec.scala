package helpers

import com.gu.multimedia.storagetier.messages.OnlineOutputMessage
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.NearlineRecordDAO
import io.circe.generic.auto._
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Try

//noinspection ScalaDeprecation
class PendingDeletionHelperSpec extends Specification with Mockito {

  "PendingDeletionHelper.storeDeletionPending" should {
    "fail if no filePath" in {

      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"nearlineId": "1",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get

      val toTest = new PendingDeletionHelper

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "Cannot store PendingDeletion record for item without filepath"
    }


    "store new record for NEARLINE if no record found" in {

      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(None)
      pendingDeletionRecordDAO.writeRecord(any) returns Future(234)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"nearlineId": "1",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get
      val expectedRec = PendingDeletionRecord(None, msgObj.originalFilePath.get, msgObj.nearlineId, msgObj.vidispineItemId, MediaTiers.NEARLINE, 1)

      val toTest = new PendingDeletionHelper

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      there was one(pendingDeletionRecordDAO).writeRecord(expectedRec)

      result must beSuccessfulTry
      result.get must beRight(234)
    }


    "store updated record for NEARLINE if record already present" in {

      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      val existingRecord = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsId"), MediaTiers.NEARLINE, 1)
      val expectedUpdatedRecordToSave = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsId"), MediaTiers.NEARLINE, 2)
      pendingDeletionRecordDAO.findByNearlineIdForNEARLINE(any) returns Future(Some(existingRecord))
      pendingDeletionRecordDAO.writeRecord(expectedUpdatedRecordToSave) returns Future(234)

      val msgContent =
        """{
          |"mediaTier": "NEARLINE",
          |"projectIds": ["22"],
          |"originalFilePath": "/srv/Multimedia2/Media Production/Assets/Multimedia_Reactive_News_and_Sport/Reactive_News_Explainers_2022/monika_cvorak_MH_Investigation/Footage Vera Productions/2022-03-18_MH.mp4",
          |"nearlineId": "1",
          |"vidispineItemId": "VX-151922",
          |"mediaCategory": "Deliverables"
          |}""".stripMargin

      val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get
      val expectedRecWithUpdatedCount = PendingDeletionRecord(existingRecord.id, existingRecord.originalFilePath, existingRecord.nearlineId, existingRecord.vidispineItemId, existingRecord.mediaTier, existingRecord.attempt + 1)

      val toTest = new PendingDeletionHelper

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      there was one(pendingDeletionRecordDAO).writeRecord(expectedRecWithUpdatedCount)

      result must beSuccessfulTry
      result.get must beRight(234)
    }
  }
}
