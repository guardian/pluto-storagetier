import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.internal.AmazonS3ExceptionBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.{TransferManager, TransferProgress, Upload}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.File

class FileUploaderSpec extends Specification with Mockito {
  "FileUploader" should {
    "Failure when file doesn't exist" in {
      val mockedS3 = mock[AmazonS3]
      val mockTransferManager = mock[TransferManager]
      val file = mock[File]
      file.getAbsolutePath returns "non-existing-file"
      file.exists returns false
      file.isFile returns false

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.copyFileToS3(file) must beAFailedTry
    }

    "Failure when Exception with wrong status code returned from S3" in {
      val file = mock[File]
      file.getAbsolutePath returns "non-existing-file"
      file.exists returns true
      file.isFile returns true

      val mockedS3 = mock[AmazonS3]
      val mockTransferManager = mock[TransferManager]
      val builder = new AmazonS3ExceptionBuilder()
      builder.setStatusCode(400)
      builder.setErrorCode("400 Unknown")

      val mockExc = builder.build()
      mockedS3.getObjectMetadata(any[String],any[String]) throws mockExc

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.copyFileToS3(file) must beAFailedTry
    }

    "File uploaded when it doesn't already exist in bucket" in {
      val file = mock[File]
      file.getAbsolutePath returns "filePath"
      file.exists returns true
      file.isFile returns true
      file.length returns 100

      val mockedS3 = mock[AmazonS3]
      val mockUpload = mock[Upload]
      val mockProgress = mock[TransferProgress]
      mockProgress.getBytesTransferred returns 100
      mockUpload.waitForCompletion()
      mockUpload.getProgress returns mockProgress
      val mockTransferManager = mock[TransferManager]
      mockTransferManager.upload(any[String], any[String], any[File]) returns mockUpload

      val builder = new AmazonS3ExceptionBuilder()
      builder.setStatusCode(404)
      builder.setErrorCode("404 Not Found")
      val mockExc = builder.build()

      mockedS3.getObjectMetadata(any[String],any[String]) throws mockExc

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.copyFileToS3(file) must beASuccessfulTry(("filePath", 100))
    }
  }

  "File uploaded with incremented name when a file with same name already exist in bucket" in {
    val file = mock[File]
    file.getAbsolutePath returns "filePath"
    file.exists returns true
    file.isFile returns true
    file.length returns 20

    val mockedS3 = mock[AmazonS3]
    val mockUpload = mock[Upload]
    val mockProgress = mock[TransferProgress]
    mockProgress.getBytesTransferred returns 20
    mockUpload.waitForCompletion()
    mockUpload.getProgress returns mockProgress
    val mockTransferManager = mock[TransferManager]
    mockTransferManager.upload(any[String], any[String], any[File]) returns mockUpload

    val builder = new AmazonS3ExceptionBuilder()
    builder.setStatusCode(404)
    builder.setErrorCode("404 Not Found")
    val mockExc = builder.build()
    val someMetadata = mock[ObjectMetadata]
    someMetadata.getContentLength returns 10

    mockedS3.getObjectMetadata(any[String],any[String]) returns someMetadata thenThrows mockExc

    val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

    fileUploader.copyFileToS3(file) must beASuccessfulTry(("filePath-1", 20))
  }

  "File not uploaded when an already existing file with same file size exists i bucket" in {
    val file = mock[File]
    file.getAbsolutePath returns "filePath"
    file.exists returns true
    file.isFile returns true
    file.length returns 10

    val mockedS3 = mock[AmazonS3]
    val mockUpload = mock[Upload]
    val mockProgress = mock[TransferProgress]
    mockProgress.getBytesTransferred returns 10
    mockUpload.waitForCompletion()
    mockUpload.getProgress returns mockProgress
    val mockTransferManager = mock[TransferManager]
    mockTransferManager.upload(any[String], any[String], any[File]) returns mockUpload

    val builder = new AmazonS3ExceptionBuilder()
    builder.setStatusCode(404)
    builder.setErrorCode("404 Not Found")
    val mockExc = builder.build()
    val someMetadata = mock[ObjectMetadata]
    someMetadata.getContentLength returns 10

    mockedS3.getObjectMetadata(any[String],any[String]) returns someMetadata thenThrows mockExc

    val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

    fileUploader.copyFileToS3(file) must beASuccessfulTry(("filePath", 10))
  }

  "FileUploader.objectExists" should {
    "return true when object exists in bucket" in {
      val mockedS3 = mock[AmazonS3]
      val someMetadata = mock[ObjectMetadata]
      mockedS3.getObjectMetadata(any, any) returns someMetadata
      val mockTransferManager = mock[TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.objectExists("my-object-key") must beASuccessfulTry(true)
    }

    "return false when object don't exist in bucket" in {
      val mockedS3 = mock[AmazonS3]
      val builder = new AmazonS3ExceptionBuilder()
      builder.setStatusCode(404)
      builder.setErrorCode("404 Not Found")
      val mockExc = builder.build()

      mockedS3.getObjectMetadata(any, any) throws mockExc
      val mockTransferManager = mock[TransferManager]

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.objectExists("my-object-key") must beASuccessfulTry(false)
    }

    "return Failure when there is an unknown error from S3" in {
      val mockedS3 = mock[AmazonS3]
      val builder = new AmazonS3ExceptionBuilder()
      builder.setStatusCode(500)
      builder.setErrorCode("Unknown error")
      val mockExc = builder.build()

      mockedS3.getObjectMetadata(any, any) throws mockExc
      val mockTransferManager = mock[TransferManager]

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
}
