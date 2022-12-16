import com.gu.multimedia.storagetier.plutocore.PlutoCoreConfig
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import java.nio.file.Paths
import java.util.concurrent.CompletableFuture
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Try

class S3ObjectCheckerSpec extends Specification with Mockito {

  "S3ObjectChecker.eTagIsMd5" should {
    "reject non-MD5 string" in {
      S3ObjectChecker.eTagIsProbablyMd5("deadbeef") mustEqual false
    }
    "reject eTag for multipart upload" in {
      S3ObjectChecker.eTagIsProbablyMd5("d41d8cd98f00b204e9800998ecf8427e-38") mustEqual false
    }
    "accept MD5 eTag" in {
      S3ObjectChecker.eTagIsProbablyMd5("599393a2c526c680119d84155d90f1e5") mustEqual true
    }
  }

  "S3ObjectChecker.objectExistsWithSizeAndMaybeChecksum" should {
    "return true if there is a pre-existing file with the same size" in {
      val mockedS3Async = mock[S3AsyncClient]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = CompletableFuture.completedFuture(ListObjectVersionsResponse.builder().versions(fakeVersions).build())
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse
      val s3ObjectChecker = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",50, None), 1.second)

      result must beTrue
    }

    "return false if there is no pre-existing file with the same size" in {
      val mockedS3Async = mock[S3AsyncClient]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = CompletableFuture.completedFuture(ListObjectVersionsResponse.builder().versions(fakeVersions).build())
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse
      val s3ObjectChecker = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",8888, None), 1.second)

      result must beFalse
    }

    "return false if the object does not exist" in {
      val mockedS3Async = mock[S3AsyncClient]

      val noSuchKeyException = NoSuchKeyException.builder().statusCode(404).build()
      val cfCompletedExceptionally = new CompletableFuture[ListObjectVersionsResponse]
      cfCompletedExceptionally.completeExceptionally(noSuchKeyException)
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns cfCompletedExceptionally
      val s3ObjectChecker = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",8888, None), 1.second)

      result must beFalse
    }

    "pass on any other exception as a Failure" in {
      val mockedS3Async = mock[S3AsyncClient]

      val notNoSuchKeyException = S3Exception.builder().statusCode(500).message("bork").build()
      val cfCompletedExceptionally = new CompletableFuture[ListObjectVersionsResponse]
      cfCompletedExceptionally.completeExceptionally(notNoSuchKeyException)
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns cfCompletedExceptionally

      val s3ObjectChecker = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Try { Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",8888, None), 1.second) }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Could not check pre-existing versions for s3://bucket/filePath: bork"
    }
  }

//    "S3ObjectChecker.mediaExistsInDeepArchive" should {
//      "relativize when called with item in path" in {
//        val fakeConfig = PlutoCoreConfig("test", "test", Paths.get("/srv/Multimedia2/NextGenDev/Media Production/Assets/"))
//
////        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
////        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
////        implicit val vidispineCommunicator = mock[VidispineCommunicator]
////        implicit val mat: Materializer = mock[Materializer]
////        implicit val sys: ActorSystem = mock[ActorSystem]
////        implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
////        implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
////        implicit val mockChecksumChecker = mock[ChecksumChecker]
//
//        implicit val mockOnlineHelper = mock[helpers.OnlineHelper]
//        implicit val mockNearlineHelper = mock[helpers.NearlineHelper]
//        implicit val mockPendingDeletionHelper = mock[helpers.PendingDeletionHelper]
//
//        val assetFolderLookup = new AssetFolderLookup(fakeConfig)
//
//        val toTest = new MediaNotRequiredMessageProcessor(assetFolderLookup)
//
//        val msgContent = """{"mediaTier":"NEARLINE","projectIds":["374"],"originalFilePath":"/srv/Multimedia2/NextGenDev/Media Production/Assets/Fred_In_Bed/This_Is_A_Test/david_allison_Deletion_Test_5/VX-3183.XML","fileSize":8823,"vidispineItemId":null,"nearlineId":"51a0f742-3d89-11ec-a895-8e29f591bdb6-2319","mediaCategory":"metadata"}"""
//
//        val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get
//
//        mockS3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(any(), any(), any()) returns Future(true)
//
//        toTest.mediaExistsInDeepArchive(msgObj.mediaTier, None, 1L, msgObj.originalFilePath.get)
//
//        there was one(mockS3ObjectChecker).objectExistsWithSizeAndMaybeChecksum("Fred_In_Bed/This_Is_A_Test/david_allison_Deletion_Test_5/VX-3183.XML", 1L, None)
//      }
//
//      "strip when called with item not in path" in {
//        val fakeConfig = PlutoCoreConfig("test", "test", Paths.get("/srv/Multimedia2/NextGenDev/Media Production/Assets/"))
//
//        implicit val pendingDeletionRecordDAO: PendingDeletionRecordDAO = mock[PendingDeletionRecordDAO]
//        implicit val nearlineRecordDAO: NearlineRecordDAO = mock[NearlineRecordDAO]
//        implicit val vidispineCommunicator = mock[VidispineCommunicator]
//        implicit val mat: Materializer = mock[Materializer]
//        implicit val sys: ActorSystem = mock[ActorSystem]
//        implicit val mockBuilder = mock[MXSConnectionBuilderImpl]
//        implicit val mockS3ObjectChecker = mock[S3ObjectChecker]
//        implicit val mockChecksumChecker = mock[ChecksumChecker]
//
//        implicit val mockOnlineHelper = mock[helpers.OnlineHelper]
//        implicit val mockNearlineHelper = mock[helpers.NearlineHelper]
//        implicit val mockPendingDeletionHelper = mock[helpers.PendingDeletionHelper]
//
//        val assetFolderLookup = new AssetFolderLookup(fakeConfig)
//
//        val toTest = new MediaNotRequiredMessageProcessor(assetFolderLookup)
//
//        val msgContent = """{"mediaTier":"NEARLINE","projectIds":["374"],"originalFilePath":"/srv/Multimedia2/NextGenDev/Proxies/VX-11976.mp4","fileSize":291354,"vidispineItemId":null,"nearlineId":"741d089d-a920-11ec-a895-8e29f591bdb6-1568","mediaCategory":"proxy"}"""
//
//        mockS3ObjectChecker.objectExistsWithSizeAndMaybeChecksum(any(), any(), any()) returns Future(true)
//
//        val msgObj = io.circe.parser.parse(msgContent).flatMap(_.as[OnlineOutputMessage]).right.get
//
//        toTest.mediaExistsInDeepArchive(msgObj.mediaTier, None, 1L, msgObj.originalFilePath.get)
//
//        there was one(mockS3ObjectChecker).objectExistsWithSizeAndMaybeChecksum("srv/Multimedia2/NextGenDev/Proxies/VX-11976.mp4", 1L, None)
//      }
//    }

}
