import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

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
      val mockedS3 = mock[S3Client]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = ListObjectVersionsResponse.builder().versions(fakeVersions).build()
      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse
      val s3ObjectChecker = new S3ObjectChecker(mockedS3, "bucket")
      s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",50, None) must beASuccessfulTry(true)
    }

    "return false if there is no pre-existing file with the same size" in {
      val mockedS3 = mock[S3Client]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = ListObjectVersionsResponse.builder().versions(fakeVersions).build()
      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse
      val s3ObjectChecker = new S3ObjectChecker(mockedS3, "bucket")
      s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",8888, None) must beASuccessfulTry(false)
    }

    "return false if the object does not exist" in {
      val mockedS3 = mock[S3Client]

      val expectedException = NoSuchKeyException.builder().statusCode(404).build()

      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) throws expectedException

      val fileUploader = new S3ObjectChecker(mockedS3, "bucket")
      fileUploader.objectExistsWithSizeAndMaybeChecksum("filePath",8888, None) must beASuccessfulTry(false)
    }

    "pass on any other exception as a Failure" in {
      val mockedS3 = mock[S3Client]

      val expectedException = S3Exception.builder().statusCode(500).build()

      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) throws expectedException

      val s3ObjectChecker = new S3ObjectChecker(mockedS3, "bucket")
      s3ObjectChecker.objectExistsWithSizeAndMaybeChecksum("filePath",8888, None) must beAFailedTry
    }
  }
}
