import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.transfer.{TransferManager}
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.{Failure, Success, Try}

class FileUploader(transferManager: TransferManager, client: AmazonS3, bucketName: String) {
  private val logger = LoggerFactory.getLogger(getClass)

  def copyFileToS3(file: File): Try[Option[String]] = {
    if (!file.exists || !file.isFile) {
      logger.info(s"File ${file.getAbsolutePath} doesn't exist")
      Failure(new Exception(s"File ${file.getAbsolutePath} doesn't exist"))
    } else {
      tryUploadFile(file)
    }
  }

  private def tryUploadFile(file: File, increment: Int = 0): Try[Option[String]] = {
    // TODO is this the correct file path?
    val filePath = file.getAbsolutePath

    val newFilePath = if (increment <= 0) filePath else {
      // check if fle has an extension and insert the increment before it if this is the case
      val pos = filePath.lastIndexOf(".")
      if (pos > 0) filePath.patch(pos, s"-$increment", 0) else s"$filePath-$increment"
    }

    Try {
      client.getObjectMetadata(bucketName, newFilePath)
    } match {
      case Success(metadata) =>
        val objectSize = metadata.getContentLength
        if (file.length == objectSize) {
          logger.info(s"Object $newFilePath already exist on S3")
          Try { Some(newFilePath) }
        } else {
          logger.warn(s"Object $newFilePath with different size already exist on S3, creating file with incremented name instead")
          tryUploadFile(file, increment + 1)
        }
      case Failure(s3e: AmazonS3Exception) =>
        if (fileNotFound(s3e)) {
          uploadFile(file, newFilePath)
        } else {
          Try { throw s3e }
        }
      case Failure(e) =>
        Try { throw e }
    }
  }

  private def fileNotFound(s3e: AmazonS3Exception): Boolean = {
    s3e.getStatusCode == 404 && s3e.getErrorCode == "404 Not Found"
  }

  private def uploadFile(file: File, keyName: String): Try[Option[String]] = {
    Try { transferManager.upload(bucketName, keyName, file).waitForCompletion }.map(_=>Some(keyName))
  }
}
