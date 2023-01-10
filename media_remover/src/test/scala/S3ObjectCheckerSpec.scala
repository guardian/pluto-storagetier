import com.gu.multimedia.storagetier.models.common.MediaTiers
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import java.util.concurrent.CompletableFuture
import scala.concurrent.{Await, Future}
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

  
  "S3ObjectChecker.onlineMediaExistsInDeepArchive" should {
    "call mediaExistsInDeepArchive with ONLINE" in {
      val mockedS3Async = mock[S3AsyncClient]
      val mockMediaExistsInDeepArchive = mock[(MediaTiers.Value, Option[String], Long, String, String) => Future[Boolean]]
      mockMediaExistsInDeepArchive.apply(any[MediaTiers.Value], any[Option[String]], anyLong, anyString, anyString) returns Future(true)

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket") {
        override def mediaExistsInDeepArchive(mediaTier: MediaTiers.Value, checksumMaybe: Option[String], fileSize: Long, originalFilePath: String, objectKey: String): Future[Boolean] =
          mockMediaExistsInDeepArchive(mediaTier, checksumMaybe, fileSize, originalFilePath, objectKey)
      }

      val result = Await.result(toTest.onlineMediaExistsInDeepArchive(Some("aChecksum"), 8888, "some/file/path", "anObjectKey"), 1.second)

      there was one(mockMediaExistsInDeepArchive).apply(MediaTiers.ONLINE, Some("aChecksum"), 8888, "some/file/path", "anObjectKey")

      result must beTrue
    }

    "onlineMediaExistsInDeepArchive gets a passed through exception" in {
      val mockedS3Async = mock[S3AsyncClient]

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket") {
        override def objectExistsWithSizeAndMaybeChecksum(objectKey: String, fileSize: Long, maybeLocalMd5: Option[String]): Future[Boolean] = Future.failed(new RuntimeException("no S3 for you"))
      }

      val result = Try { Await.result(toTest.onlineMediaExistsInDeepArchive(Some("aChecksum"), 8888, "some/file/path", "anObjectKey"), 1.second) }

      result must beFailedTry
    }
  }


  "S3ObjectChecker.nearlineMediaExistsInDeepArchive" should {
    "call mediaExistsInDeepArchive with NEARLINE" in {
      val mockedS3Async = mock[S3AsyncClient]
      val mockMediaExistsInDeepArchive = mock[(MediaTiers.Value, Option[String], Long, String, String) => Future[Boolean]]
      mockMediaExistsInDeepArchive.apply(any[MediaTiers.Value], any[Option[String]], anyLong, anyString, anyString) returns Future(true)

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket") {
        override def mediaExistsInDeepArchive(mediaTier: MediaTiers.Value, checksumMaybe: Option[String], fileSize: Long, originalFilePath: String, objectKey: String): Future[Boolean] =
          mockMediaExistsInDeepArchive(mediaTier, checksumMaybe, fileSize, originalFilePath, objectKey)
      }

      val result = Await.result(toTest.nearlineMediaExistsInDeepArchive(Some("aChecksum"), 8888, "some/file/path", "anObjectKey"), 1.second)

      there was one(mockMediaExistsInDeepArchive).apply(MediaTiers.NEARLINE, Some("aChecksum"), 8888, "some/file/path", "anObjectKey")

      result must beTrue
    }
  }


  "S3ObjectChecker.objectExistsWithSizeAndMaybeChecksum" should {
    "return true if there is a pre-existing file with the same size, and eTag is not an MD5, so checksum matching is skipped" in {
      val mockedS3Async = mock[S3AsyncClient]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).eTag("someNonMD5Checksum").build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).eTag("otherNonMD5Checksum").build(),
      ).asJavaCollection
      val fakeVersionsResponse = CompletableFuture.completedFuture(ListObjectVersionsResponse.builder().versions(fakeVersions).build())
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(toTest.objectExistsWithSizeAndMaybeChecksum("filePath", 50, Some("someChecksum")), 1.second)

      result must beTrue
    }

    "return false if there is a pre-existing file with the same size, and eTag is MD5, but checksums do not match" in {
      val mockedS3Async = mock[S3AsyncClient]

      val someChecksum = "07ce8816e0217b02568ef03612fc6207"
      val otherChecksum = "5de25b4a60941798f4349a1e0f8c4e56"
      val yetAnotherChecksum = "8b841ddccc2a819cf1726e171947a54e"

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).eTag(someChecksum).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).eTag(otherChecksum).build(),
      ).asJavaCollection
      val fakeVersionsResponse = CompletableFuture.completedFuture(ListObjectVersionsResponse.builder().versions(fakeVersions).build())
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(toTest.objectExistsWithSizeAndMaybeChecksum("filePath", 50, Some(yetAnotherChecksum)), 1.second)

      result must beFalse
    }

    "return true if there is a pre-existing file with the same size, and eTag is MD5, and checksums do match" in {
      val mockedS3Async = mock[S3AsyncClient]

      val someChecksum = "07ce8816e0217b02568ef03612fc6207"
      val otherChecksum = "5de25b4a60941798f4349a1e0f8c4e56"

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).eTag(someChecksum).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).eTag(otherChecksum).build(),
      ).asJavaCollection
      val fakeVersionsResponse = CompletableFuture.completedFuture(ListObjectVersionsResponse.builder().versions(fakeVersions).build())
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(toTest.objectExistsWithSizeAndMaybeChecksum("filePath", 50, Some(someChecksum)), 1.second)

      result must beTrue
    }

    "return true if there is a pre-existing file with the same size and there is no local checksum" in {
      val mockedS3Async = mock[S3AsyncClient]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = CompletableFuture.completedFuture(ListObjectVersionsResponse.builder().versions(fakeVersions).build())
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(toTest.objectExistsWithSizeAndMaybeChecksum("filePath", 50, None), 1.second)

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

      val result = Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath", 8888, None), 1.second)

      result must beFalse
    }

    "return false if the object does not exist" in {
      val mockedS3Async = mock[S3AsyncClient]

      val noSuchKeyException = NoSuchKeyException.builder().statusCode(404).build()
      val cfCompletedExceptionally = new CompletableFuture[ListObjectVersionsResponse]
      cfCompletedExceptionally.completeExceptionally(noSuchKeyException)
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns cfCompletedExceptionally
      val s3ObjectChecker = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath", 8888, None), 1.second)

      result must beFalse
    }

    "pass on any other exception as a Failure" in {
      val mockedS3Async = mock[S3AsyncClient]

      val notNoSuchKeyException = S3Exception.builder().statusCode(500).message("bork").build()
      val cfCompletedExceptionally = new CompletableFuture[ListObjectVersionsResponse]
      cfCompletedExceptionally.completeExceptionally(notNoSuchKeyException)
      mockedS3Async.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns cfCompletedExceptionally

      val s3ObjectChecker = new S3ObjectChecker(mockedS3Async, "bucket")

      val result = Try {
        Await.result(s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath", 8888, None), 1.second)
      }

      result must beAFailedTry
      result.failed.get.getMessage mustEqual "Could not check pre-existing versions for s3://bucket/filePath: bork"
    }
  }


  "S3ObjectChecker.mediaExistsInDeepArchive" should {
    "return future true if exists" in {
      val mockedS3Async = mock[S3AsyncClient]

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket") {
        override def objectExistsWithSizeAndMaybeChecksum(objectKey: String, fileSize: Long, maybeLocalMd5: Option[String]): Future[Boolean] = Future(true)
      }

      val result = Await.result(toTest.mediaExistsInDeepArchive(MediaTiers.NEARLINE, Some("aChecksum"), 8888, "some/file/path", "anObjectKey"), 1.second)

      result must beTrue
    }

    "return future false if not exists" in {
      val mockedS3Async = mock[S3AsyncClient]

      val toTest = new S3ObjectChecker(mockedS3Async, "bucket") {
        override def objectExistsWithSizeAndMaybeChecksum(objectKey: String, fileSize: Long, maybeLocalMd5: Option[String]): Future[Boolean] = Future(false)
      }

      val result = Await.result(toTest.mediaExistsInDeepArchive(MediaTiers.NEARLINE, Some("aChecksum"), 8888, "some/file/path", "anObjectKey"), 1.second)

      result must beFalse
    }

    "forwards exception if objectExistsWithSizeAndMaybeChecksum throws exception" in {
      val mockedS3Async = mock[S3AsyncClient]
      val bucketName = "aBucket"
      val objectKey = "anObjectKey"

      val toTest = new S3ObjectChecker(mockedS3Async, bucketName) {

        override def objectExistsWithSizeAndMaybeChecksum(objectKey: String, fileSize: Long, maybeLocalMd5: Option[String]): Future[Boolean] =
          Future.failed(new RuntimeException(s"Could not check pre-existing versions for s3://$bucketName/$objectKey: some message"))
      }

      val result = Try { Await.result(toTest.mediaExistsInDeepArchive(MediaTiers.NEARLINE, Some("aChecksum"), 8888, "some/file/path", objectKey), 1.second) }

      result must beAFailedTry
      result.failed.get must beAnInstanceOf[RuntimeException]
      result.failed.get.getMessage mustEqual "Could not check pre-existing versions for s3://aBucket/anObjectKey: some message"
    }
  }

}
