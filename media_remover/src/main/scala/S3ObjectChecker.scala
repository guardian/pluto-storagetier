import org.slf4j.{LoggerFactory, MDC}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.transfer.s3.S3ClientConfiguration

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class S3ObjectChecker(client: S3Client, var bucketName: String)(implicit ec:ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  import S3ObjectChecker._

  def objectExistsWithSizeAndMaybeChecksum(objectKey: String, fileSize: Long, maybeNearlineMd5: Option[String]): Try[Boolean] = {
    logger.info(s"Checking for existing versions of s3://$bucketName/$objectKey with size $fileSize and nearline checksum $maybeNearlineMd5")
    val req = ListObjectVersionsRequest.builder().bucket(bucketName).prefix(objectKey).build()

    Try { client.listObjectVersions(req) } match {
      case Success(response)=>
        val versions = response.versions().asScala
        logger.info(s"s3://$bucketName/$objectKey has ${versions.length} versions")
        versions.foreach(v=>logger.info(s"s3://$bucketName/$objectKey @${v.versionId()} with size ${v.size()}, ETag ${v.eTag()} and checksum algorithm ${v.checksumAlgorithmAsStrings()} (has checksum alg: ${v.hasChecksumAlgorithm})"))
        val matchesForSize = versions.filter(_.size()==fileSize)

        if(matchesForSize.isEmpty) {
          logger.info(s"Found no entries for s3://$bucketName/$objectKey with size $fileSize, new copy must be required")
          Success(false)
        } else {
          /* If we have a nearline MD5, and
             we have ETags that are MD5s
             then, the nearline MD5 must match one of the ETags.
             If not all ETags are simple MD5s, we skip this check and are satisfied by the fact that the objectKey and size match.
           */
          maybeNearlineMd5 match {
            case Some(nearlineMd5) =>
              val matchesWithMd5 = matchesForSize.filter(m => eTagIsProbablyMd5(m.eTag))
              if (matchesWithMd5.isEmpty) {
                Success(true)
              } else {
                matchesWithMd5.count(_.eTag().equals(nearlineMd5)) match { // TODO Check if both checksums are encoded the same way (hex, yes?)
                  case 0 => Success(false)
                  case _ => Success(true)
                }
              }
            case None =>
              logger.info(s"Found ${matchesForSize.length} existing entries for s3://$bucketName/$objectKey with size $fileSize, safe to delete from nearline")
              Success(true)
          }
        }
      case Failure(_:NoSuchKeyException)=>Success(false)
      case Failure(err)=>
        logger.error(s"Could not check pre-existing versions for s3://$bucketName/$objectKey: ${err.getMessage}", err)
        Failure(err)
    }
  }
}

object S3ObjectChecker {
  private def s3ClientConfig = {
    val b = S3ClientConfiguration.builder()
    val withRegion = sys.env.get("AWS_REGION") match {
      case Some(rgn)=>b.region(Region.of(rgn))
      case None=>b
    }
    withRegion.build()
  }

  private def initS3Client = wrapJavaMethod(()=>{
    val b = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder())

    val withRegion = sys.env.get("AWS_REGION") match {
      case Some(rgn)=>b.region(Region.of(rgn))
      case None=>b
    }
    withRegion.build()
  })

  private def wrapJavaMethod[A](blk: ()=>A) = Try { blk() }.toEither.left.map(_.getMessage)

  def createFromEnvVars(varName:String)(implicit ec:ExecutionContext):Either[String, S3ObjectChecker] =
    sys.env.get(varName) match {
      case Some(mediaBucket) =>
        for {
          s3Client <- initS3Client
          result <- Right(new S3ObjectChecker(s3Client, mediaBucket))
        } yield result
      case None =>
        Left(s"You must specify $varName so we know where uploads are!")
    }

  def eTagIsProbablyMd5(m: String): Boolean = {
    m.length == 32 && m.forall(c => "abcdefABCDEF0123456789".contains(c))
  }
}
