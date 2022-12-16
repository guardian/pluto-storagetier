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

class NearlineHelperSpec extends Specification with Mockito{
  implicit val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

    "NearlineHelper.dealWithAttFiles" should {
      "handle failed fromOID" in {

        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
        implicit val failureRecordDAO: FailureRecordDAO = mock[FailureRecordDAO]
        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
        implicit val vidispineCommunicator = mock[VidispineCommunicator]
        implicit val mat: Materializer = mock[Materializer]
        implicit val sys: ActorSystem = mock[ActorSystem]
        implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//        implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
        implicit val mockChecksumChecker = mock[ChecksumChecker]

        implicit val mockOnlineHelper = mock[OnlineHelper]
        implicit val mockNearlineHelper = mock[NearlineHelper]
        implicit val mockPendingDeletionHelper = mock[PendingDeletionHelper]

        val mockAssetFolderLookup = mock[AssetFolderLookup]

        val vault = mock[Vault]
        vault.getId returns "mockVault"

        val filePath = "/path/to/some/file.ext"

        val toTest = new NearlineHelper(mockAssetFolderLookup) {
          override protected def callObjectMatrixEntryFromOID(vault: Vault, fileName: String) = Future.fromTry(Failure(new RuntimeException("no oid for you")))
        }

        val result = Await.result(toTest.dealWithAttFiles(vault, "556593b10503", filePath), 2.seconds)

        result must beLeft("Something went wrong while trying to get metadata to delete ATT files for 556593b10503: java.lang.RuntimeException: no oid for you")
      }
    }
}
