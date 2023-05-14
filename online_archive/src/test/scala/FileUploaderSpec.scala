import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{HeadObjectRequest, HeadObjectResponse, ListObjectVersionsRequest, ListObjectVersionsResponse, NoSuchKeyException, ObjectVersion, PutObjectResponse, S3Exception}
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.{CompletedUpload, Upload, UploadRequest}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.File
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.jdk.FutureConverters._

class FileUploaderSpec extends Specification with Mockito {
  "FileUploader" should {
    "Failure when file doesn't exist" in {
      val mockedS3 = mock[S3Client]
      val mockTransferManager = mock[S3TransferManager]
      val file = mock[File]
      file.getAbsolutePath returns "non-existing-file"
      file.exists returns false
      file.isFile returns false

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      Try { Await.result(fileUploader.copyFileToS3(file), 2.seconds) } must beAFailedTry
    }

    "Failure when Exception with wrong status code returned from S3" in {
      val file = mock[File]
      file.getAbsolutePath returns "non-existing-file"
      file.exists returns true
      file.isFile returns true

      val mockedS3 = mock[S3Client]
      val mockTransferManager = mock[S3TransferManager]

      mockedS3.headObject(org.mockito.ArgumentMatchers.any[HeadObjectRequest]) throws NoSuchKeyException.builder().build()

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      Try { Await.result(fileUploader.copyFileToS3(file), 2.seconds) } must beAFailedTry
    }

    "File uploaded when it doesn't already exist in bucket" in {
      val file = mock[File]
      file.getAbsolutePath returns "filePath"
      file.exists returns true
      file.isFile returns true
      file.length returns 100

      val fakeUploadedInfo = HeadObjectResponse.builder().contentLength(100).build()
      val mockedS3 = mock[S3Client]
      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = ListObjectVersionsResponse.builder().versions(fakeVersions).build()

      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      //we are not doing an initial headObject check any more, HeadObject is now only done _after_ upload.
      mockedS3.headObject(org.mockito.ArgumentMatchers.any[HeadObjectRequest]) returns fakeUploadedInfo

      val putResponse = PutObjectResponse.builder().build()
      val completedUpload = CompletedUpload.builder().response(putResponse).build()
      val mockUpload = mock[Upload]
      mockUpload.completionFuture() returns Future(completedUpload).asJava.toCompletableFuture
      val mockTransferManager = mock[S3TransferManager]
      mockTransferManager.upload(org.mockito.ArgumentMatchers.any[UploadRequest]) returns mockUpload

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      val result = Try { Await.result(fileUploader.copyFileToS3(file), 2.seconds) }
      there was one(mockTransferManager).upload(org.mockito.ArgumentMatchers.any[UploadRequest])
      result must beASuccessfulTry(("filePath", 100))
    }
  }

  "File not uploaded when an already existing file with same file size exists i bucket" in {
    val file = mock[File]
    file.getAbsolutePath returns "filePath"
    file.exists returns true
    file.isFile returns true
    file.length returns 10

    val mockedS3 = mock[S3Client]

    val fakeVersions = Seq(
      ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
      ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
    ).asJavaCollection
    val fakeVersionsResponse = ListObjectVersionsResponse.builder().versions(fakeVersions).build()

    mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

    val putResponse = PutObjectResponse.builder().build()
    val completedUpload = CompletedUpload.builder().response(putResponse).build()
    val mockUpload = mock[Upload]
    mockUpload.completionFuture() returns Future(completedUpload).asJava.toCompletableFuture
    val mockTransferManager = mock[S3TransferManager]
    mockTransferManager.upload(org.mockito.ArgumentMatchers.any[UploadRequest]) returns mockUpload

    val someMetadata = HeadObjectResponse.builder().contentLength(10).build()

    val expectedExc = NoSuchKeyException.builder().statusCode(404).build()
    mockedS3.headObject(org.mockito.ArgumentMatchers.any[HeadObjectRequest]) returns someMetadata thenThrows expectedExc

    val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

    there was no(mockTransferManager).upload(org.mockito.ArgumentMatchers.any[UploadRequest])
    Try { Await.result(fileUploader.copyFileToS3(file), 2.seconds) } must beASuccessfulTry(("filePath", 10))
  }

  "FileUploader.objectExists" should {
    "return true when object exists in bucket" in {
      val mockedS3 = mock[S3Client]
      val someMetadata = HeadObjectResponse.builder().build()
      mockedS3.headObject(org.mockito.ArgumentMatchers.any[HeadObjectRequest]) returns someMetadata

      val mockTransferManager = mock[S3TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.objectExists("my-object-key") must beASuccessfulTry(true)
    }

    "return false when object don't exist in bucket" in {
      val mockedS3 = mock[S3Client]
      val expectedExc = NoSuchKeyException.builder().statusCode(404).build()
      mockedS3.headObject(org.mockito.ArgumentMatchers.any[HeadObjectRequest]) throws expectedExc

      val mockTransferManager = mock[S3TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.objectExists("my-object-key") must beASuccessfulTry(false)
    }

    "return Failure when there is an unknown error from S3" in {
      val mockExc = S3Exception.builder().statusCode(500).build()

      val mockedS3 = mock[S3Client]
      mockedS3.headObject(org.mockito.ArgumentMatchers.any[HeadObjectRequest]) throws mockExc

      val mockTransferManager = mock[S3TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.objectExists("my-object-key") must beAFailedTry
    }
  }

  "FileUploader.vsMD5ToS3" should {
    "convert hex string to base64 string" in {
      FileUploader.vsMD5toS3MD5("deadbeef") must beASuccessfulTry("3q2+7w==")
    }

    "fail on bad data" in {
      FileUploader.vsMD5toS3MD5("world") must beAFailedTry
    }
  }

  "FileUploader.calculateChunkSize" should {
    "return a larger chunk size for a larger file" in {
      //100Gb file
      FileUploader.calculateChunkSize(107374182400L) must beGreaterThan(52428800L)
    }

    "return a smaller chunk size for a smaller file" in {
      //15Mb file
      FileUploader.calculateChunkSize(15728640L) must beLessThan(52428800L)
    }

    "return the default chunk size for a medium size file" in {
      //200Mb file
      FileUploader.calculateChunkSize(209715200L) mustEqual 52428800L
    }

    "return no less than 5Mb chunk size" in {
      //1Mb file (shouldn't get this but ensure it does not crash
      FileUploader.calculateChunkSize(1048576L) mustEqual(5242880L)
    }
  }

  "FileUploader.objectExistsWithSize" should {
    "return true if there is a pre-existing file" in {
      val mockedS3 = mock[S3Client]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = ListObjectVersionsResponse.builder().versions(fakeVersions).build()
      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      val mockTransferManager = mock[S3TransferManager]


      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")
      fileUploader.objectExistsWithSize("filePath",50) must beASuccessfulTry(true)
    }

    "return false if there is no pre-existing file" in {
      val mockedS3 = mock[S3Client]

      val fakeVersions = Seq(
        ObjectVersion.builder().key("filePath").versionId("abcdefg").size(50).build(),
        ObjectVersion.builder().key("filePath").versionId("xyzzqt").size(10).build(),
      ).asJavaCollection
      val fakeVersionsResponse = ListObjectVersionsResponse.builder().versions(fakeVersions).build()
      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) returns fakeVersionsResponse

      val mockTransferManager = mock[S3TransferManager]


      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")
      fileUploader.objectExistsWithSize("filePath",8888) must beASuccessfulTry(false)
    }

    "return false if the object does not exist" in {
      val mockedS3 = mock[S3Client]

      val expectedException = NoSuchKeyException.builder().statusCode(404).build()

      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) throws expectedException

      val mockTransferManager = mock[S3TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")
      fileUploader.objectExistsWithSize("filePath",8888) must beASuccessfulTry(false)
    }

    "pass on any other exception as a Failure" in {
      val mockedS3 = mock[S3Client]

      val expectedException = S3Exception.builder().statusCode(500).build()

      mockedS3.listObjectVersions(org.mockito.ArgumentMatchers.any[ListObjectVersionsRequest]) throws expectedException

      val mockTransferManager = mock[S3TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")
      fileUploader.objectExistsWithSize("filePath",8888) must beAFailedTry
    }
  }
}
