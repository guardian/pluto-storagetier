import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.transfer.{TransferManagerBuilder, Upload}
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.{Success, Failure, Try}

class FileUploader(client: AmazonS3, bucketName: String) {
  private val logger = LoggerFactory.getLogger(getClass)

  def copyFileToS3(fileName: String): Try[Option[String]] = Try {
    val file: File = new File(fileName)

    if (!file.exists || !file.isFile) {
      logger.info(s"File $fileName doesn't exist")
      return Try(None)
    }

    return Try(tryUploadFile(file))
  }

  private def tryUploadFile(file: File, increment: Int = 0): Option[String] = {
    // TODO is this the correct file path?
    val filePath = file.getAbsolutePath
    val newFilePath = if (increment > 0) s"$filePath-$increment" else filePath

    Try {
      client.getObjectMetadata(bucketName, newFilePath)
    } match {
      case Success(metadata) =>
        val objectSize = metadata.getContentLength
        if (file.length == objectSize) {
          logger.info(s"Object $newFilePath already exist on S3")
          return None
        }

        logger.warn(s"Object $newFilePath with different size already exist on S3, creating file with incremented name instead")
        tryUploadFile(file, increment + 1)

      case Failure(e) =>
        e match {
          case _: AmazonS3Exception =>
            uploadFile(file, newFilePath)
          case _ => throw e
        }
    }
  }

  private def uploadFile(file: File, keyName: String): Option[String] = {
    val transferManager = TransferManagerBuilder.standard.withS3Client(client).build

    try {
      val transfer: Upload = transferManager.upload(bucketName, keyName, file)
      transfer.waitForCompletion()
      Some(keyName)
    } finally {
      transferManager.shutdownNow()
    }
  }
}
