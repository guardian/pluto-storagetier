import akka.actor.ActorSystem
import akka.stream.Materializer
import com.gu.multimedia.mxscopy.MXSConnectionBuilderImpl
import com.gu.multimedia.storagetier.messages.AssetSweeperNewFile
import com.gu.multimedia.storagetier.models.nearline_archive.{FailureRecordDAO, NearlineRecord, NearlineRecordDAO}
import com.gu.multimedia.storagetier.vidispine.VidispineCommunicator
import com.om.mxs.client.japi.{MxsObject, Vault}
import matrixstore.MatrixStoreConfig
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import io.circe.syntax._
import io.circe.generic.auto._

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class AssetSweeperMessageProcessorSpec extends Specification with Mockito {
  implicit val mxsConfig = MatrixStoreConfig(Array("127.0.0.1"), "cluster-id", "mxs-access-key", "mxs-secret-key", "vault-id", None)

  "AssetSweeperMessageProcessor.processFile" should {
    "perform an upload and record success if record doesn't already exist" in {
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(None)
      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      fileCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Right("some-object-id"))
      val mockCheckForPreExistingFiles = mock[(Vault, AssetSweeperNewFile)=>Future[Option[NearlineRecord]]]
      mockCheckForPreExistingFiles.apply(any,any) returns Future(None)

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject

      val toTest = new AssetSweeperMessageProcessor() {
        override protected def checkForPreExistingFiles(vault: Vault, file: AssetSweeperNewFile): Future[Option[NearlineRecord]] = mockCheckForPreExistingFiles(vault, file)
      }
      val mockFile = mock[AssetSweeperNewFile]
      mockFile.filepath returns "/path/to/Assets/project"
      mockFile.filename returns "original-file.mov"

      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)

      val rec: NearlineRecord = NearlineRecord(
        id = Some(123),
        objectId = "some-object-id",
        originalFilePath = "/path/to/Assets/project/original-file.mov",
        vidispineItemId = None,
        vidispineVersionId = None,
        proxyObjectId = None,
        metadataXMLObjectId = None
      )

      result.map(value=>value) must beRight(rec.asJson)
      there was one(mockCheckForPreExistingFiles).apply(mockVault, mockFile)
    }

    "perform an upload and record success if record with objectId doesn't exist in ObjectMatrix" in {
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      val rec: NearlineRecord = NearlineRecord(
        id = Some(123),
        objectId = "some-object-id",
        originalFilePath = "/path/to/Assets/project/original-file.mov",
        vidispineItemId = None,
        vidispineVersionId = None,
        proxyObjectId = None,
        metadataXMLObjectId = None
      )
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(rec))

      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]
      fileCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Right("some-object-id"))

      val mockVault = mock[Vault]
      //workaround from https://stackoverflow.com/questions/3762047/throw-checked-exceptions-from-mocks-with-mockito
      mockVault.getObject(any) answers( (x:Any)=> throw new IOException("Invalid object, it does not exist (error 306)"))

      val toTest = new AssetSweeperMessageProcessor()
      val mockFile = mock[AssetSweeperNewFile]
      mockFile.filepath returns "/path/to/Assets/project"
      mockFile.filename returns "original-file.mov"

      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)

      result must beRight(rec.asJson)
    }

    "return Failure if Left is returned when trying to copy file ObjectMatrix" in {
      implicit val nearlineRecordDAO:NearlineRecordDAO = mock[NearlineRecordDAO]
      implicit val vidispineCommunicator = mock[VidispineCommunicator]

      val rec: NearlineRecord = NearlineRecord(
        id = Some(123),
        objectId = "some-object-id",
        originalFilePath = "/path/to/Assets/project/original-file.mov",
        vidispineItemId = None,
        vidispineVersionId = None,
        proxyObjectId = None,
        metadataXMLObjectId = None
      )
      nearlineRecordDAO.writeRecord(any) returns Future(123)
      nearlineRecordDAO.findBySourceFilename(any) returns Future(Some(rec))

      implicit val failureRecordDAO:FailureRecordDAO = mock[FailureRecordDAO]
      failureRecordDAO.writeRecord(any) returns Future(234)

      implicit val mat:Materializer = mock[Materializer]
      implicit val sys:ActorSystem = mock[ActorSystem]
      implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
      implicit val fileCopier = mock[FileCopier]

      val mockExc = new RuntimeException("ObjectMatrix out of office right now!!")
      fileCopier.copyFileToMatrixStore(any, any, any, any) returns Future(Left(s"ObjectMatrix error: ${mockExc.getMessage}"))

      val mockVault = mock[Vault]
      mockVault.getObject(any) throws mockExc

      val toTest = new AssetSweeperMessageProcessor()
      val mockFile = mock[AssetSweeperNewFile]
      mockFile.filepath returns "/path/to/Assets/project"
      mockFile.filename returns "original-file.mov"

      val result = Await.result(toTest.processFile(mockFile, mockVault), 3.seconds)

      result must beLeft("ObjectMatrix error: ObjectMatrix out of office right now!!")
    }
  }
}
