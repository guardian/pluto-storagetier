package helpers

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMessage
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.{ChecksumChecker, MXSConnectionBuilderImpl}
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.storagetier.framework.{MessageProcessingFramework, MessageProcessorReturnValue, RMQDestination, SilentDropMessage}
import com.gu.multimedia.storagetier.framework.MessageProcessorConverters._
import com.gu.multimedia.storagetier.messages.{AssetSweeperNewFile, OnlineOutputMessage, VidispineField, VidispineMediaIngested}
import com.gu.multimedia.storagetier.models.common.MediaTiers
import com.gu.multimedia.storagetier.models.media_remover.{PendingDeletionRecord, PendingDeletionRecordDAO}
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.EntryStatus
import messages.MediaRemovedMessage

import scala.language.postfixOps
import scala.util.{Failure, Success}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, PlutoCoreConfig, ProjectRecord}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import helpers.{NearlineHelper, OnlineHelper, PendingDeletionHelper}
import io.circe.generic.auto._
import io.circe.syntax._
import matrixstore.MatrixStoreConfig
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.IOException
import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

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

      val toTest = new PendingDeletionHelper

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      result must beSuccessfulTry
      result.get must beRight(234)
    }


    "store updated record for NEARLINE if record already present" in {

      implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
      implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
      val existingRecord = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsid"), MediaTiers.NEARLINE, 1)
      val expectedUpdatedRecordToSave = PendingDeletionRecord(Some(234), "some/file/path", Some("nearline-test-id"), Some("vsid"), MediaTiers.NEARLINE, 2)
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

      val toTest = new PendingDeletionHelper

      val result = Try {
        Await.result(toTest.storeDeletionPending(msgObj), 2.seconds)
      }

      result must beSuccessfulTry
      result.get must beRight(234)
    }
  }
}
