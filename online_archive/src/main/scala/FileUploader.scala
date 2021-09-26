import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.transfer.{TransferManager, Upload}
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.{Failure, Success, Try}

class FileUploader(transferManager: TransferManager, client: AmazonS3, bucketName: String) {
  private val logger = LoggerFactory.getLogger(getClass)

  def copyFileToS3(file: File): Try[Option[String]] = Try {
    if (!file.exists || !file.isFile) {
      logger.info(s"File ${file.getAbsolutePath} doesn't exist")
      throw new Exception(s"File ${file.getAbsolutePath} doesn't exist")
    }

    return Try(tryUploadFile(file))
  }

  private def tryUploadFile(file: File, increment: Int = 0): Option[String] = {
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
          return Some(newFilePath)
        }

        logger.warn(s"Object $newFilePath with different size already exist on S3, creating file with incremented name instead")
        tryUploadFile(file, increment + 1)

      case Failure(e) =>
        e match {
          case s3e: AmazonS3Exception =>
            if (s3e.getStatusCode == 404 && s3e.getErrorCode == "404 Not Found") {
              uploadFile(file, newFilePath)
            } else {
              throw e
            }
          case _ => throw e
        }
    }
  }

  private def uploadFile(file: File, keyName: String): Option[String] = {
    val transfer: Upload = transferManager.upload(bucketName, keyName, file)
    transfer.waitForCompletion()
    Some(keyName)
  }
}
