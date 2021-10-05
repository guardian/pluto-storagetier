import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.slf4j.LoggerFactory

import java.io.File
import scala.util.{Failure, Success, Try}

class FileUploader(transferManager: TransferManager, client: AmazonS3, var bucketName: String) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Attempt to copy the given file to S3 under a distinct name.
   * If a file with the same name AND the same file size exists, then returns a Success with the found file name
   * If no file with the name exists, will upload it and return a Success with the file name as uploaded.
   * If a file with the same name but a DIFFERENT file size exists, will suffix the name "-1", "-2" etc. until a 'free'
   * filename is found. The file is then uploaded and a Success returned with a Tuple containing the the uploaded file name and file
   * size.
   * If there is an error, then Failure is returned
   * @param file a java.io.File instance representing the file to upload
   * @param maybeUploadPath Optional destination path to upload it to. If not set, then the absolute path of `file` is used.
   * @return a Try, containing a Tuple where the first value is a String containing the uploaded file name and the second value is a
   *         Long containing the file size.
   */
  def copyFileToS3(file: File, maybeUploadPath:Option[String]=None): Try[(String, Long)] = {
    if (!file.exists || !file.isFile) {
      logger.info(s"File ${file.getAbsolutePath} doesn't exist")
      Failure(new Exception(s"File ${file.getAbsolutePath} doesn't exist"))
    } else {
      tryUploadFile(file, maybeUploadPath.getOrElse(file.getAbsolutePath).stripPrefix("/"))
    }
  }

  /**
   * internal method. Recursively checks for an existing file and starts the upload when it finds a 'free' filename
   * @param file java.nio.File to upload
   * @param filePath path to upload it to
   * @param increment iteration number
   * @return
   */
  private def tryUploadFile(file: File, filePath:String, increment: Int = 0): Try[(String, Long)] = {
    val newFilePath = if (increment <= 0) filePath else {
      // check if file has an extension and insert the increment before it if this is the case
      val pos = filePath.lastIndexOf(".")
      if (pos > 0) filePath.patch(pos, s"-$increment", 0) else s"$filePath-$increment"
    }

    Try {
      client.getObjectMetadata(bucketName, newFilePath)
    } match {
      case Success(metadata) =>
        val objectSize = metadata.getContentLength
        if (file.length == objectSize) {
          logger.info(s"Object $newFilePath already exists on S3")
          Success((newFilePath, objectSize))
        } else {
          logger.warn(s"Object $newFilePath with different size already exist on S3, creating file with incremented name instead")
          tryUploadFile(file, filePath, increment + 1)
        }
      case Failure(s3e: AmazonS3Exception) =>
        if (fileNotFound(s3e)) {
          uploadFile(file, newFilePath)
        } else {
          Failure(s3e)
        }
      case Failure(e) =>
        Failure(e)
    }
  }

  /**
   * returns `true` if the given AmazonS3Exception represents "file not found
   * @param s3e AmazonS3Exception to inspect
   * @return
   */
  private def fileNotFound(s3e: AmazonS3Exception): Boolean = {
    s3e.getStatusCode == 404 && s3e.getErrorCode == "404 Not Found"
  }

  /**
   * performs an upload via S3 TransferManager, blocking the current thread until it is ready
   * @param file java.nio.file to upload
   * @param keyName S3 key name to upload to
   * @return
   */
  private def uploadFile(file: File, keyName: String): Try[(String, Long)] = {
    Try {
      val upload = transferManager.upload(bucketName, keyName, file)
      upload.waitForCompletion
      val bytes = upload.getProgress.getBytesTransferred

      (keyName, bytes)
    }
  }
}

object FileUploader {
  private def initTransferManager = wrapJavaMethod(()=>TransferManagerBuilder.defaultTransferManager())
  private def initS3Client = wrapJavaMethod(()=>AmazonS3ClientBuilder.defaultClient())

  private def wrapJavaMethod[A](blk: ()=>A) = Try { blk() }.toEither.left.map(_.getMessage)

  def createFromEnvVars:Either[String, FileUploader] =
    sys.env.get("ARCHIVE_MEDIA_BUCKET") match {
      case Some(mediaBucket) =>
        for {
          transferManager <- initTransferManager
          s3Client <- initS3Client
          result <- Right(new FileUploader(transferManager, s3Client, mediaBucket))
        } yield result
      case None =>
        Left("You must specify ARCHIVE_MEDIA_BUCKET so we know where to upload to!")
    }
}