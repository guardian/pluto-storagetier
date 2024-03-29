import akka.http.scaladsl.model.{ContentType, Uri}
import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import org.apache.commons.codec.binary.Hex
import org.slf4j.{LoggerFactory, MDC}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.UploadRequest

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}
import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class FileUploader(transferManager: S3TransferManager, client: S3Client, var bucketName: String)(implicit ec:ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  import FileUploader._
  protected def AwsRequestBodyFromFile(file:File) = AsyncRequestBody.fromFile(file)

  /**
   * Attempt to copy the given file to S3 under a distinct name.
   * If a file with the same name AND the same file size exists, then returns a Success with the found file name.
   * If a file with the same name but a DIFFERENT file size exists, the file is then uploaded and a Success returned
   * with a tuple containing the file name and file size.
   * If no file with the name exists, will upload it and return a Success with the file name as uploaded.
   * If there is an error, then Failure is returned.
   * @param file a java.io.File instance representing the file to upload
   * @param maybeUploadPath Optional destination path to upload it to. If not set, then the absolute path of `file` is used.
   * @return a Try, containing a Tuple where the first value is a String containing the uploaded file name and the second value is a
   *         Long containing the file size.
   */
  def copyFileToS3(file: File, maybeUploadPath: Option[String] = None): Future[(String, Long)] = {
    if (!file.exists || !file.isFile) {
      logger.info(s"File ${file.getAbsolutePath} doesn't exist")
      Future.failed(new RuntimeException(s"File ${file.getAbsolutePath} doesn't exist"))
    } else {
      val uploadName = maybeUploadPath.getOrElse(file.getAbsolutePath).stripPrefix("/")
      val fileSize = file.length()
      objectExistsWithSize(uploadName, fileSize) match {
        case Success(true) =>
          logger.info(s"No upload needed for ${file.getAbsolutePath} as a matching copy already exists")
          Future.successful((uploadName, file.length()))
        case Success(false) =>
          logger.info(s"Uploading ${file.getAbsolutePath} as there is no previously matching copy")
          uploadFile(file, uploadName).map(response => (uploadName, response.contentLength().toLong))
        case Failure(err) =>
          Future.failed(err)
      }
    }
  }


  private def getObjectMetadata(bucketName: String, key: String) = Try {
    val req = HeadObjectRequest.builder().bucket(bucketName).key(key).build()
    client.headObject(req)
  }

  /**
   * Check if an object key exists in the FileUploader bucket.
   * @param objectKey key to the s3 object
   * @return a Try containing a Success with boolean indicating if the object exist or not or a Failure if the call failed for some
   *         unknown reason.
   */
  def objectExists(objectKey: String): Try[Boolean] = {
    getObjectMetadata(bucketName, objectKey) match {
      case Success(_) => Success(true)
      case Failure(_: NoSuchKeyException) => Success(false)
      case Failure(e) => Failure(e)
    }
  }

  /**
   * Checks if a version of the given object key exists with the specified file size
   * @param objectKey key to check
   * @param fileSize file size that must match
   * @return a Try that fails if the underlying S3 operation fails. Otherwise it returns `true` if there is a pre-existing
   *         version and `false` if not.
   */
  def objectExistsWithSize(objectKey:String, fileSize:Long): Try[Boolean] = {
    logger.info(s"Checking for existing versions of s3://$bucketName/$objectKey with size $fileSize")
    val req = ListObjectVersionsRequest.builder().bucket(bucketName).prefix(objectKey).build()

    Try { client.listObjectVersions(req) } match {
      case Success(response)=>
        val versions = response.versions().asScala
        logger.info(s"s3://$bucketName/$objectKey has ${versions.length} versions")
        versions.foreach(v=>logger.info(s"s3://$bucketName/$objectKey @${v.versionId()} with size ${v.size()} and checksum ${v.checksumAlgorithmAsStrings()}"))
        val matches = versions.filter(_.size()==fileSize)
        if(matches.nonEmpty) {
          logger.info(s"Found ${matches.length} existing entries for s3://$bucketName/$objectKey with size $fileSize, not creating a new one")
          Success(true)
        } else {
          logger.info(s"Found no entries for s3://$bucketName/$objectKey with size $fileSize, copy is required")
          Success(false)
        }
      case Failure(_:NoSuchKeyException)=>Success(false)
      case Failure(err)=>
        logger.error(s"Could not check pre-existing versions for s3://$bucketName/$objectKey: ${err.getMessage}", err)
        Failure(err)
    }
  }
  /**
   * internal method. Recursively checks for an existing file and starts the upload when it finds a 'free' filename
   * @param file java.nio.File to upload
   * @param filePath path to upload it to
   * @param increment iteration number
   * @return
   */
  private def tryUploadFile(file: File, filePath:String, increment: Int = 0): Future[(String, Long)] = {
    for {
      copyInfo <- Future.fromTry(findFreeFilename(file.length(), filePath, increment))
      uploadedObject <- if(copyInfo._3) { //should copy==true
        val uploadName = copyInfo._1
        uploadFile(file, uploadName).map(response=> (uploadName, response.contentLength().toLong) ) //scala long !== java long so we must convert java long to scala long here
      } else {
        Future( (copyInfo._1, copyInfo._2) )
      }
    } yield uploadedObject
  }


  /**
   * Internal method, that recursively finds a free filename to upload to, or will return a successful upload
   * if a file matching the given length and size exists remotely
   * @param contentLength length of the file in question
   * @param filePath file path to try
   * @param increment counter, set to 0 by default. Don't specify this when calling.
   * @return a Try with a 3-tuple consisting of (found_filename, found_filesize, should_copy).
   */
  private def findFreeFilename(contentLength:Long, filePath:String, increment: Int = 0): Try[(String, Long, Boolean)] = {
    val newFilePath = if (increment <= 0) filePath else {
      // check if file has an extension and insert the increment before it if this is the case
      val pos = filePath.lastIndexOf(".")
      if (pos > 0) filePath.patch(pos, s"-$increment", 0) else s"$filePath-$increment"
    }

    getObjectMetadata(bucketName, newFilePath) match {
      case Success(metadata) =>
        val objectSize = metadata.contentLength()
        if (contentLength == objectSize) {
          logger.info(s"Object $newFilePath already exists on S3")
          Success((newFilePath, objectSize, false))
        } else {
          logger.warn(s"Object $newFilePath with different size already exist on S3, creating file with incremented name instead")
          findFreeFilename(contentLength, filePath, increment + 1)
        }
      case Failure(_: NoSuchKeyException) => Success((newFilePath, 0, true))
      case Failure(e) =>
        Failure(e)
    }
  }


  /**
   * performs an upload via S3 TransferManager
   * @param file java.nio.file to upload
   * @param keyName S3 key name to upload to
   * @return a Future that completes with a HeadObjectResponse once the upload is completed.
   */

  def calculateMD5(file: File): Try[String] = {
    Try {
      val buffer = new Array[Byte](8192)
      val md5 = MessageDigest.getInstance("MD5")
      val dis = new DigestInputStream(new FileInputStream(file), md5)

      try {
        while (dis.read(buffer) != -1) {}
      } finally {
        dis.close()
      }

      Base64.getEncoder.encodeToString(md5.digest())
    }
  }

  private def uploadFile(file: File, keyName: String, contentType: Option[String] = None): Future[HeadObjectResponse] = {
    import scala.jdk.FutureConverters._
    val loggerContext = Option(MDC.getCopyOfContextMap)

    val md5Result = calculateMD5(file)

    md5Result match {
      case Success(md5) =>
        logger.info(s"Calculated MD5 for $file: $md5")

        val basePutReq = PutObjectRequest.builder()
          .bucket(bucketName)
          .key(keyName)
          .contentMD5(md5)

        val putReq = contentType match {
          case Some(contentType) => basePutReq.contentType(contentType)
          case None => basePutReq
        }

        logger.info(s"Put request for $file: ${putReq.build().toString}")

        val response = for {
          job <- Future.fromTry(Try {
            val md5 = calculateMD5(file).get
            val req = UploadRequest.builder().putObjectRequest(putReq.contentMD5(md5).build()).requestBody(AwsRequestBodyFromFile(file))
              .build()
            logger.info(s"Request used by S3TransferManager: $req")

            transferManager.upload(req)
          })
          _ <- job.completionFuture().asScala
          result <- Future.fromTry(getObjectMetadata(bucketName, keyName))
        } yield result

        response.recover {
          case e: S3Exception =>
            logger.error(s"S3Exception when uploading $file: ${e.getMessage}", e)
            throw e
          case e =>
            logger.error(s"Other exception when uploading $file: ${e.getMessage}", e)
            throw e
        }.map(result => {
          loggerContext.map(MDC.setContextMap) //restore the logger context if it was set
          result
        })
      case Failure(e) =>
        logger.error(s"Failed to calculate MD5 for $file: ${e.getMessage}", e)
        Future.failed(e)
    }
  }

  private def generateS3Uri(bucket:String, keyToUpload:String) = {
    val fixedKeyToUpload = if(keyToUpload.startsWith("/")) keyToUpload else "/" + keyToUpload //fix "could not generate URI" error
    Uri().withScheme("s3").withHost(bucket).withPath(Uri.Path(fixedKeyToUpload))
  }

  def uploadAkkaStreamViaTempfile(src:Source[ByteString, Any], keyName:String, contentType:ContentType)(implicit mat:Materializer, ec:ExecutionContext) = {
    val tempFilePath = Files.createTempFile(Paths.get(keyName).getFileName.toString, ".temp")
    val resultFuture = for {
      copyResult <- src.runWith(FileIO.toPath(tempFilePath))
      _ <- Future.fromTry(copyResult.status)
      result <- uploadFile(tempFilePath.toFile, keyName, Some(contentType.toString()))
    } yield result

   resultFuture.andThen({case _=>Files.delete(tempFilePath)})
  }
}

object FileUploader {
  private def s3ClientConfig = {
    val b = S3CrtAsyncClient.builder().minimumPartSizeInBytes(104857600) // 100MB = 100 * 1024 * 1024 = 104857600
    val withRegion = sys.env.get("AWS_REGION") match {
      case Some(rgn)=>b.region(Region.of(rgn))
      case None=>b
    }
    withRegion.build()
  }
  private def initTransferManager = wrapJavaMethod(()=>
    S3TransferManager.builder()
      .s3Client(s3ClientConfig)
      .build()
  )

  private def initS3Client = wrapJavaMethod(()=>{
    val b = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder())

    val withRegion = sys.env.get("AWS_REGION") match {
      case Some(rgn)=>b.region(Region.of(rgn))
      case None=>b
    }
    withRegion.build()
  })

  private def wrapJavaMethod[A](blk: ()=>A) = Try { blk() }.toEither.left.map(_.getMessage)

  def createFromEnvVars(varName:String)(implicit ec:ExecutionContext):Either[String, FileUploader] =
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
