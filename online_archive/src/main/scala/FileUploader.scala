import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.transfer.{TransferManagerBuilder, Upload}
import org.slf4j.LoggerFactory

import java.io.File

object FileUploader {
  private val logger = LoggerFactory.getLogger(getClass)

  def copyFileToS3(client: AmazonS3, bucketName: String, fileName: String): Unit = {
    val file: File = new File(fileName)

    if (!file.exists || !file.isFile)
      return

    // TODO is this the correct path
    val filePath = file.getAbsolutePath

    // Check if a matching (path and size) file exists in S3
    try {
      val metadata = client.getObjectMetadata(bucketName, filePath)
      val objectSize = metadata.getContentLength

      if (file.length == objectSize) {
        logger.info(s"File $fileName already exist on S3")
        return
      }

      logger.warn(s"File $fileName with different file size already exist on S3, creating file with incremented name instead")
      tryUploadFileName(client, bucketName, filePath, 1)
    } catch {
      case _: AmazonS3Exception =>
        // no file exists upload file
        uploadFile(client, bucketName, file, filePath)
    }
  }

  private def tryUploadFileName(client: AmazonS3, bucketName: String, fileName: String, increment: Int): Unit = {
    val file: File = new File(fileName)
    // TODO is this the correct file path?
    val filePath = file.getAbsolutePath

    val newFilePath = s"$filePath-$increment"
    try {
      client.getObjectMetadata(bucketName, newFilePath)

      // file already exist, try next increment
      tryUploadFileName(client, bucketName, fileName, increment + 1)
    } catch {
      case _: AmazonS3Exception =>
        // no file exists upload file
        uploadFile(client, bucketName, file, newFilePath)
    }
  }

  private def uploadFile(client: AmazonS3, bucketName: String, file: File, keyName: String): Unit = {
    val transferManager = TransferManagerBuilder.standard.withS3Client(client).build

    try {
      val transfer: Upload = transferManager.upload(bucketName, keyName, file)
      try {
        transfer.waitForCompletion()
        logger.info(s"Successfully uploaded file $keyName to S3 $bucketName")
      } catch {
        case e: AmazonServiceException => logger.error(s"Failed to upload file $keyName to S3 $bucketName. ${e.getMessage}", e)
        case e: AmazonClientException => logger.error(s"Failed to upload file $keyName to S3 $bucketName. ${e.getMessage}", e)
        case e: InterruptedException => logger.error(s"Failed to upload file $keyName to S3 $bucketName. ${e.getMessage}", e)
      }
    } catch {
      case e: AmazonServiceException =>
        logger.error(s"Failed to upload file $keyName to S3 $bucketName. ${e.getMessage}", e)
    } finally {
      transferManager.shutdownNow()
    }
  }
}
