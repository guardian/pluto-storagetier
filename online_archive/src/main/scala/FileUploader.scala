import akka.http.scaladsl.model.{ContentType, Uri}
import akka.stream.Materializer
import akka.stream.alpakka.s3.{MultipartUploadResult, S3Headers}
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import java.io.{File, InputStream}
import java.util.Base64
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class FileUploader(transferManager: TransferManager, client: AmazonS3, var bucketName: String) {
  private val logger = LoggerFactory.getLogger(getClass)
  import FileUploader._

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
   * uploads the given stream. Prior existing file is over-written (or reversioned, depending on bucket settings)
   * @param stream InputStream to write
   * @param keyName key to write within the bucket
   * @param mimeType MIME type
   * @param size size of the data that will be streamed.  While this is optional, it's highly recommended as the S3 Transfer library will buffer the whole contents into memory in this case
   * @param vsMD5
   * @return
   */
  @deprecated("Use uploadAkkaStream instead of an InputStream")
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

    try {
      val upload = transferManager.upload(bucketName, keyName, stream, meta)
      upload.waitForCompletion()
      (keyName, upload.getProgress.getBytesTransferred)
    } finally {
      stream.close()
    }
  }

  def uploadAkkaStream(src:Source[ByteString, Any], keyName:String, contentType:ContentType, sizeHint:Option[Long], customHeaders:Map[String,String]=Map())(implicit mat:Materializer, ec:ExecutionContext) = {
    val baseHeaders = S3Headers.empty
    val applyHeaders = if(customHeaders.nonEmpty) baseHeaders.withCustomHeaders(customHeaders) else baseHeaders

    sizeHint match {
      case Some(sizeHint)=>
        if(sizeHint>5242880) {
          val chunkSize = calculateChunkSize(sizeHint).toInt
          logger.info(s"SizeHint is $sizeHint, preferring multipart upload with chunk size ${chunkSize/1048576}Mb")
          src
            .runWith(S3.multipartUploadWithHeaders(bucketName, keyName, contentType, chunkSize=chunkSize, s3Headers=applyHeaders))
        } else {
          logger.info(s"SizeHint is $sizeHint (less than 5Mb), preferring single-hit upload")
          S3
            .putObject(bucketName, keyName, src, sizeHint, contentType, s3Headers=applyHeaders)
            .runWith(Sink.head)
            .map(objectMetadata=>MultipartUploadResult(Uri().withScheme("s3").withHost(bucketName).withPath(Uri.Path(keyName)),
              bucketName,
              keyName,
              objectMetadata.eTag.getOrElse(""),
              objectMetadata.versionId)
            )
        }
      case None=>
        logger.warn(s"No sizeHint has been specified for s3://$bucketName/$keyName. Trying default multipart upload, this may fail!")
        src.runWith(S3.multipartUploadWithHeaders(bucketName, keyName, contentType, s3Headers=applyHeaders))
    }

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
        Left(s"You must specify $varName so we know where to upload to!")
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
   * AWS documentation states that there must be <10,000 parts in a multipart upload and each part must be
   * between 5Mb and 5Gb. The last part can be shorter. This method tries to find a "good" value to use as the chunk
   * size
   * @param sizeHint actual size of the file to upload
   */
  def calculateChunkSize(sizeHint:Long, startingChunkSize:Long=50*1024*1024):Long = {
    val proposedChunkCount:Double = sizeHint / startingChunkSize
    if(proposedChunkCount>1000) { //if we end up with a LOT of chunks then we should increase the chunk size. AWS recommends larger chunks rather than more of them.
      calculateChunkSize(sizeHint, startingChunkSize*2)
    } else if(proposedChunkCount<1) { //if we end up with less than one chunk we should reduce the chunk size
      calculateChunkSize(sizeHint, sizeHint/2)
    } else if(startingChunkSize<5242880L) {  //if we end up with < 5Mb then just use that
      5242880L
    } else {
      startingChunkSize
    }
  }
}