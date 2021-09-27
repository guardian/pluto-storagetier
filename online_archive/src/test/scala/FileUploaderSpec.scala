import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.internal.AmazonS3ExceptionBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.File
import scala.util.Try

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

      val mockedS3 = mock[AmazonS3]
      val mockUpload = mock[Upload]
      mockUpload.waitForCompletion()
      val mockTransferManager = mock[TransferManager]
      mockTransferManager.upload(any[String], any[String], any[File]) returns mockUpload

      val builder = new AmazonS3ExceptionBuilder()
      builder.setStatusCode(404)
      builder.setErrorCode("404 Not Found")
      val mockExc = builder.build()

      mockedS3.getObjectMetadata(any[String],any[String]) throws mockExc

      val fileUploader = new FileUploader(mockTransferManager, mockedS3, "bucket")

      fileUploader.copyFileToS3(file) must beASuccessfulTry(Some("filePath"))
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
    mockUpload.waitForCompletion()
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

    fileUploader.copyFileToS3(file) must beASuccessfulTry(Some("filePath-1"))
  }

  "File not uploaded when an already existing file with same file size exists i bucket" in {
    val file = mock[File]
    file.getAbsolutePath returns "filePath"
    file.exists returns true
    file.isFile returns true
    file.length returns 10

    val mockedS3 = mock[AmazonS3]
    val mockUpload = mock[Upload]
    mockUpload.waitForCompletion()
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

    fileUploader.copyFileToS3(file) must beASuccessfulTry(Some("filePath"))
  }
}
