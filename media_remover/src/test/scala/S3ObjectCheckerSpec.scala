import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

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
}
