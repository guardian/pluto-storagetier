import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.gu.multimedia.storagetier.framework.{MessageProcessorReturnValue, RMQDestination, SilentDropMessage}
import com.gu.multimedia.storagetier.models.nearline_archive.{NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.plutocore.{AssetFolderLookup, CommissionRecord, PlutoCoreConfig, ProjectRecord, WorkingGroupRecord}
import com.gu.multimedia.storagetier.vidispine.{ItemResponseSimplified, VidispineCommunicator}
import com.om.mxs.client.japi.{MxsObject, Vault}
import matrixstore.MatrixStoreConfig
import org.mockito.ArgumentMatcher
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.syntax._
import io.circe.generic.auto._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.nio.file.Paths
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class OwnMessageProcessorSpec extends Specification with Mockito {
  val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", Some("internal-archive-vault"))

  "OwnMessageProcessor.applyCustomMetadata" should {
    "generate metadata, request to write it down onto the given item and return a Right" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMat = mock[Materializer]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val rec = NearlineRecord(
        "some-object-id",
        "/path/to/Assets/project/original-file.mov",
        "corrId"
      )
      val fakeProject = mock[ProjectRecord]
      fakeProject.id returns Some(1234)
      fakeProject.title returns "test project"
      fakeProject.commissionId returns Some(2345)
      fakeProject.sensitive returns None
      val fakeComm = mock[CommissionRecord]
      fakeComm.id returns Some(2345)
      fakeComm.title returns "test commission"
      fakeComm.workingGroupId returns 3
      val fakeWg = mock[WorkingGroupRecord]
      fakeWg.id returns Some(3)
      fakeWg.name returns "test working group"

      val mockWriteMetadata = mock[(MxsObject, MxsMetadata, NearlineRecord)=>Either[String, NearlineRecord]]
      mockWriteMetadata.apply(any,any,any) returns Right(rec)
      val mockLookup = mock[AssetFolderLookup]
      mockLookup.assetFolderProjectLookup(any) returns Future(Some(fakeProject))
      mockLookup.optionCommissionLookup(any) returns Future(Some(fakeComm))
      mockLookup.optionWGLookup(any) returns Future(Some(fakeWg))
      implicit val vsCommunicator = mock[VidispineCommunicator]
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new OwnMessageProcessor(mxsConfig, mockLookup, "fake-output-exchange") {
        override protected def writeMetadataToObject(mxsObject: MxsObject, md: MxsMetadata, rec: NearlineRecord): Either[String, NearlineRecord] = mockWriteMetadata(mxsObject, md, rec)
      }

      val result = Await.result(toTest.applyCustomMetadata(rec, mockVault), 3.seconds)

      /*
      custom matcher to validate that we got the right content
       */
      class checkMxsMetadata extends ArgumentMatcher[MxsMetadata] {
        override def matches(argument: MxsMetadata): Boolean = {
          argument.stringValues.get("GNM_PROJECT_ID").contains("1234") &&
            argument.stringValues.get("GNM_COMMISSION_ID").contains("2345") &&
            argument.stringValues.get("GNM_PROJECT_NAME").contains("test project") &&
            argument.stringValues.get("GNM_COMMISSION_NAME").contains("test commission") &&
            argument.stringValues.get("GNM_WORKING_GROUP_NAME").contains("test working group") &&
            argument.stringValues.get("GNM_TYPE").contains("rushes")
        }
      }

      there was one(mockVault).getObject("some-object-id")
      there was one(mockLookup).assetFolderProjectLookup(Paths.get("/path/to/Assets/project/original-file.mov"))
      there was one(mockLookup).optionCommissionLookup(Some(2345))
      there was one(mockLookup).optionWGLookup(Some(3))
      there was one(mockWriteMetadata).apply(
        org.mockito.ArgumentMatchers.eq(mockObject),
        org.mockito.ArgumentMatchers.argThat(new checkMxsMetadata),
        org.mockito.ArgumentMatchers.eq(rec)
      )
      result must beRight(MessageProcessorReturnValue(rec.asJson))
    }

    "if the project is sensitive, output two messages" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMat = mock[Materializer]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val rec = NearlineRecord(
        "some-object-id",
        "/path/to/Assets/project/original-file.mov",
        "corrId"
      )
      val fakeProject = mock[ProjectRecord]
      fakeProject.id returns Some(1234)
      fakeProject.title returns "test project"
      fakeProject.commissionId returns Some(2345)
      fakeProject.sensitive returns Some(true)
      val fakeComm = mock[CommissionRecord]
      fakeComm.id returns Some(2345)
      fakeComm.title returns "test commission"
      fakeComm.workingGroupId returns 3
      val fakeWg = mock[WorkingGroupRecord]
      fakeWg.id returns Some(3)
      fakeWg.name returns "test working group"

      val mockWriteMetadata = mock[(MxsObject, MxsMetadata, NearlineRecord)=>Either[String, NearlineRecord]]
      mockWriteMetadata.apply(any,any,any) returns Right(rec)
      val mockLookup = mock[AssetFolderLookup]
      mockLookup.assetFolderProjectLookup(any) returns Future(Some(fakeProject))
      mockLookup.optionCommissionLookup(any) returns Future(Some(fakeComm))
      mockLookup.optionWGLookup(any) returns Future(Some(fakeWg))
      implicit val vsCommunicator = mock[VidispineCommunicator]
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new OwnMessageProcessor(mxsConfig, mockLookup, "fake-output-exchange") {
        override protected def writeMetadataToObject(mxsObject: MxsObject, md: MxsMetadata, rec: NearlineRecord): Either[String, NearlineRecord] = mockWriteMetadata(mxsObject, md, rec)
      }

      val result = Await.result(toTest.applyCustomMetadata(rec, mockVault), 3.seconds)

      /*
      custom matcher to validate that we got the right content
       */
      class checkMxsMetadata extends ArgumentMatcher[MxsMetadata] {
        override def matches(argument: MxsMetadata): Boolean = {
          argument.stringValues.get("GNM_PROJECT_ID").contains("1234") &&
            argument.stringValues.get("GNM_COMMISSION_ID").contains("2345") &&
            argument.stringValues.get("GNM_PROJECT_NAME").contains("test project") &&
            argument.stringValues.get("GNM_COMMISSION_NAME").contains("test commission") &&
            argument.stringValues.get("GNM_WORKING_GROUP_NAME").contains("test working group") &&
            argument.stringValues.get("GNM_TYPE").contains("rushes")
        }
      }

      there was one(mockVault).getObject("some-object-id")
      there was one(mockLookup).assetFolderProjectLookup(Paths.get("/path/to/Assets/project/original-file.mov"))
      there was one(mockLookup).optionCommissionLookup(Some(2345))
      there was one(mockLookup).optionWGLookup(Some(3))
      there was one(mockWriteMetadata).apply(
        org.mockito.ArgumentMatchers.eq(mockObject),
        org.mockito.ArgumentMatchers.argThat(new checkMxsMetadata),
        org.mockito.ArgumentMatchers.eq(rec)
      )
      result must beRight(MessageProcessorReturnValue(rec.asJson, Seq(RMQDestination("fake-output-exchange", "storagetier.nearline.internalarchive.required"))))
    }

    "return Left for a retry if the metadata lookup fails" in {
      implicit val mockActorSystem = mock[ActorSystem]
      implicit val mockMat = mock[Materializer]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]

      val rec = NearlineRecord(
        "some-object-id",
        "/path/to/Assets/project/original-file.mov",
        "corrId"
      )
      val fakeProject = mock[ProjectRecord]
      fakeProject.id returns Some(1234)
      fakeProject.title returns "test project"
      fakeProject.commissionId returns Some(2345)
      val fakeWg = mock[WorkingGroupRecord]
      fakeWg.id returns Some(3)
      fakeWg.name returns "test working group"

      val mockWriteMetadata = mock[(MxsObject, MxsMetadata, NearlineRecord)=>Either[String, NearlineRecord]]
      mockWriteMetadata.apply(any,any,any) returns Right(rec)
      val mockLookup = mock[AssetFolderLookup]
      mockLookup.assetFolderProjectLookup(any) returns Future(Some(fakeProject))
      mockLookup.optionCommissionLookup(any) returns Future.failed(new RuntimeException("kaboom"))
      mockLookup.optionWGLookup(any) returns Future(Some(fakeWg))
      implicit val vsCommunicator = mock[VidispineCommunicator]
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new OwnMessageProcessor(mxsConfig, mockLookup, "fake-output-exchange") {
        override protected def writeMetadataToObject(mxsObject: MxsObject, md: MxsMetadata, rec: NearlineRecord): Either[String, NearlineRecord] = mockWriteMetadata(mxsObject, md, rec)
      }

      val result = Await.result(toTest.applyCustomMetadata(rec, mockVault), 3.seconds)

      there was no(mockVault).getObject(any)
      there was one(mockLookup).assetFolderProjectLookup(Paths.get("/path/to/Assets/project/original-file.mov"))
      there was one(mockLookup).optionCommissionLookup(Some(2345))
      there was no(mockLookup).optionWGLookup(any)
      there was no(mockWriteMetadata).apply(any,any,any)
      result must beLeft
    }
  }

  "OwnMessageProcessor.handleSuccessfulMetadataWrite" should {
    "look up the record in the datastore by id, call VidispineCommunicator to write it to the item and return a Right" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.setGroupedMetadataValue(any, any,any,any) returns Future(Some(mock[ItemResponseSimplified]))
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      val fakeRecord = NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234), vidispineItemId = Some("VX-123"), vidispineVersionId=Some(1))
      nearlineRecordDAO.getRecord(any) returns Future(Some(fakeRecord))
      val asLookup = mock[AssetFolderLookup]

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name")
      val result = Await.result(toTest.handleSuccessfulMetadataWrite(NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234)).asJson), 2.seconds)

      there was one(nearlineRecordDAO).getRecord(1234)
      there was one(vsCommunicator).setGroupedMetadataValue("VX-123", "Asset", "gnm_nearline_id","some-object-id")
      result must beRight
    }

    "return a Left if VidispineCommunicator fails" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.setGroupedMetadataValue(any,any,any,any) returns Future.failed(new RuntimeException("something broke"))
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      val fakeRecord = NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234), vidispineItemId = Some("VX-123"), vidispineVersionId=Some(1))
      nearlineRecordDAO.getRecord(any) returns Future(Some(fakeRecord))
      val asLookup = mock[AssetFolderLookup]

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name")
      val result = Await.result(toTest.handleSuccessfulMetadataWrite(NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234)).asJson), 2.seconds)

      there was one(nearlineRecordDAO).getRecord(1234)
      there was one(vsCommunicator).setGroupedMetadataValue("VX-123", "Asset","gnm_nearline_id","some-object-id")
      result must beLeft
    }

    "return a Left if the item does not exist in VS" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.setGroupedMetadataValue(any, any,any,any) returns Future(None)
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      val fakeRecord = NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234), vidispineItemId = Some("VX-123"), vidispineVersionId=Some(1))
      nearlineRecordDAO.getRecord(any) returns Future(Some(fakeRecord))
      val asLookup = mock[AssetFolderLookup]

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name")
      val result = Await.result(toTest.handleSuccessfulMetadataWrite(NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234)).asJson), 2.seconds)

      there was one(nearlineRecordDAO).getRecord(1234)
      there was one(vsCommunicator).setGroupedMetadataValue("VX-123", "Asset", "gnm_nearline_id","some-object-id")
      result must beLeft
    }

    //"return Left if no vidispine ID present" is no longer the behaviour of the code, a Right is returned in this case as well
    //since we now ensure that the MXS ID is written to the item following media ingest

    "return a failed Future if the record does not exist in the datastore" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      implicit val vsCommunicator = mock[VidispineCommunicator]
      vsCommunicator.setMetadataValue(any,any,any) returns Future(Some(mock[ItemResponseSimplified]))
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.getRecord(any) returns Future(None)
      val asLookup = mock[AssetFolderLookup]

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name")
      val result = Try { Await.result(toTest.handleSuccessfulMetadataWrite(NearlineRecord("some-object-id","/path/to/original/file", "corrId").copy(id=Some(1234)).asJson), 2.seconds) }

      there was one(nearlineRecordDAO).getRecord(1234)
      there was no(vsCommunicator).setGroupedMetadataValue(any,any,any,any)
      result must beFailedTry
    }
  }

  "OwnMessageProcessor.handleInternalArchiveRequested" should {
    "initiate connection to both vaults and request data copy" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      val mockSourceVault = mock[Vault]
      val mockDestVault = mock[Vault]

      mxsConnectionBuilder.withVaultsFuture(any[Seq[String]])(any[Seq[Vault]=>Future[Any]])(any[ExecutionContext]) answers((args:Array[AnyRef])=>{
        val requestedVaultIds = args.head.asInstanceOf[Seq[String]]
        val callback = args(1).asInstanceOf[Seq[Vault]=>Future[Any]]
        callback(Seq(mockSourceVault, mockDestVault))
      })

      implicit val vsCommunicator = mock[VidispineCommunicator]

      val rec = NearlineRecord("source-object-id","/path/to/original-file", "corrId").copy(id=Some(1234))
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.setInternallyArchived(any,any) returns Future(Some(rec.copy(internallyArchived = Some(true))))
      val asLookup = mock[AssetFolderLookup]

      val mockCrossCopy = mock[(Vault, String, Vault)=>Future[(String, Option[String])]]
      mockCrossCopy.apply(any,any,any) returns Future(("destination-oid", None))

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name") {
        override protected def callCrossCopy(nearlineVault: Vault, sourceOID:String, destVault: Vault): Future[(String, Option[String])] = mockCrossCopy(nearlineVault, sourceOID, destVault)

        override def isCopyNeeded(nearlineVault: Vault, destVault: Vault, rec: NearlineRecord): Future[Boolean] = Future(true)
      }

      val result = Await.result(toTest.handleInternalArchiveRequested(rec.asJson), 2.seconds)

      result must beRight
      result.flatMap(_.content.as[NearlineRecord]) must beRight(NearlineRecord("source-object-id","/path/to/original-file", "corrId").copy(id=Some(1234), internallyArchived = Some(true)))
      there was one(mockCrossCopy).apply(mockSourceVault, "source-object-id", mockDestVault)
      there was one(mxsConnectionBuilder).withVaultsFuture(org.mockito.ArgumentMatchers.eq(Seq("vault-id", "internal-archive-vault")))(any)(any)
      there was one(nearlineRecordDAO).setInternallyArchived(1234, true)
    }

    "not try to copy a file if isCopyNeeded is false" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      val mockSourceVault = mock[Vault]
      val mockDestVault = mock[Vault]

      mxsConnectionBuilder.withVaultsFuture(any[Seq[String]])(any[Seq[Vault]=>Future[Any]])(any[ExecutionContext]) answers((args:Array[AnyRef])=>{
        val requestedVaultIds = args.head.asInstanceOf[Seq[String]]
        val callback = args(1).asInstanceOf[Seq[Vault]=>Future[Any]]
        callback(Seq(mockSourceVault, mockDestVault))
      })

      implicit val vsCommunicator = mock[VidispineCommunicator]

      val rec = NearlineRecord("source-object-id","/path/to/original-file", "corrId").copy(id=Some(1234))
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.setInternallyArchived(any,any) returns Future(Some(rec.copy(internallyArchived = Some(true))))
      val asLookup = mock[AssetFolderLookup]

      val mockCrossCopy = mock[(Vault, String, Vault)=>Future[(String, Option[String])]]
      mockCrossCopy.apply(any,any,any) returns Future(("destination-oid", None))

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name") {
        override protected def callCrossCopy(nearlineVault: Vault, sourceOID:String, destVault: Vault): Future[(String, Option[String])] = mockCrossCopy(nearlineVault, sourceOID, destVault)

        override def isCopyNeeded(nearlineVault: Vault, destVault: Vault, rec: NearlineRecord): Future[Boolean] = Future(false)
      }

      val result = Try { Await.result(toTest.handleInternalArchiveRequested(rec.asJson), 2.seconds) }

      result must beAFailedTry(SilentDropMessage(Some("/path/to/original-file is already archived")))
      there was no(mockCrossCopy).apply(mockSourceVault, "source-object-id", mockDestVault)
      there was one(mxsConnectionBuilder).withVaultsFuture(org.mockito.ArgumentMatchers.eq(Seq("vault-id", "internal-archive-vault")))(any)(any)
      there was no(nearlineRecordDAO).setInternallyArchived(1234, true)
    }

    "return a Left if the copy fails" in {
      implicit val actorSystem = mock[ActorSystem]
      implicit val mat = mock[Materializer]
      implicit val mxsConnectionBuilder = mock[MXSConnectionBuilderImpl]
      val mockSourceVault = mock[Vault]
      val mockDestVault = mock[Vault]

      mxsConnectionBuilder.withVaultsFuture(any[Seq[String]])(any[Seq[Vault]=>Future[Any]])(any[ExecutionContext]) answers((args:Array[AnyRef])=>{
        val requestedVaultIds = args.head.asInstanceOf[Seq[String]]
        val callback = args(1).asInstanceOf[Seq[Vault]=>Future[Any]]
        callback(Seq(mockSourceVault, mockDestVault))
      })

      implicit val vsCommunicator = mock[VidispineCommunicator]
      implicit val nearlineRecordDAO = mock[NearlineRecordDAO]
      val asLookup = mock[AssetFolderLookup]

      val mockCrossCopy = mock[(Vault, String, Vault)=>Future[(String, Option[String])]]
      mockCrossCopy.apply(any,any,any) returns Future.failed(new RuntimeException("Kaboom"))

      val toTest = new OwnMessageProcessor(mxsConfig, asLookup, "own-exchange-name") {
        override protected def callCrossCopy(nearlineVault: Vault, sourceOID:String, destVault: Vault): Future[(String, Option[String])] = mockCrossCopy(nearlineVault, sourceOID, destVault)

        override def isCopyNeeded(nearlineVault: Vault, destVault: Vault, rec: NearlineRecord): Future[Boolean] = Future(true)
      }

      val rec = NearlineRecord("source-object-id","/path/to/original-file", "corrId").copy(id=Some(1234))
      val result = Await.result(toTest.handleInternalArchiveRequested(rec.asJson), 2.seconds)

      result must beLeft("Kaboom")
      there was one(mockCrossCopy).apply(mockSourceVault, "source-object-id", mockDestVault)
      there was one(mxsConnectionBuilder).withVaultsFuture(org.mockito.ArgumentMatchers.eq(Seq("vault-id", "internal-archive-vault")))(any)(any)
    }
  }
}
