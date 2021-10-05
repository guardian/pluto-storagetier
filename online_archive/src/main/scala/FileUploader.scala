import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import java.io.{File, InputStream}
import java.util.Base64
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
   * Check if an object key exists in the FileUploader bucket.
   * @param objectKey key to the s3 object
   * @return a Try containing a Success with boolean indicating if the object exist or not or a Failure if the call failed for some
   *         unknown reason.
   */
  def objectExists(objectKey: String): Try[Boolean] = {
    Try {
      client.getObjectMetadata(bucketName, objectKey)
    } match {
      case Success(_) => Success(true)
      case Failure(s3e: AmazonS3Exception) =>
        if (fileNotFound(s3e)) {
          Success(false)
        } else {
          Failure(s3e)
        }
      case Failure(e) =>
        Failure(e)
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
  private def uploadFile(file: File, keyName: String): Try[(String, Long)] = Try {
      val upload = transferManager.upload(bucketName, keyName, file)
      upload.waitForCompletion
      val bytes = upload.getProgress.getBytesTransferred

      (keyName, bytes)
    }

  /**
   * converts a Vidispine md5 checksum (hex bytes string) to an S3 compatible one (base64-encoded bytes in a string)
   * @param vsMD5 incoming hex-string checksum
   * @return Base64 encoded string wrapped in a Try
   */
  def vsMD5toS3MD5(vsMD5:String) = Try {
    Base64.getEncoder.encodeToString(Hex.decodeHex(vsMD5))
  }

  /**
   * uploads the given stream. Prior existing file is over-written (or reversioned, depending on bucket settings)
   * @param stream InputStream to write
   * @param keyName key to write within the bucket
   * @param mimeType MIME type
   * @param size
   * @param vsMD5
   * @return
   */
  def uploadStreamNoChecks(stream:InputStream, keyName:String, mimeType:String, size:Option[Long], vsMD5:Option[String]): Try[(String, Long)] = Try {
    val meta = new ObjectMetadata()
    meta.setContentType(mimeType)
    size.map(meta.setContentLength)

    vsMD5.map(vsMD5toS3MD5) match {
      case None=>
      case Some(Failure(err))=>
        logger.error(s"Could not convert vidispine MD5 value $vsMD5 to base64: $err")
      case Some(Success(b64))=>
        meta.setContentMD5(b64)
    }

    val upload = transferManager.upload(bucketName, keyName, stream, meta)
    upload.waitForCompletion()
    (keyName, upload.getProgress.getBytesTransferred)
  }
}

object FileUploader {
  private def initTransferManager = wrapJavaMethod(()=>TransferManagerBuilder.defaultTransferManager())
  private def initS3Client = wrapJavaMethod(()=>AmazonS3ClientBuilder.defaultClient())

  private def wrapJavaMethod[A](blk: ()=>A) = Try { blk() }.toEither.left.map(_.getMessage)

  def createFromEnvVars(varName:String):Either[String, FileUploader] =
    sys.env.get(varName) match {
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