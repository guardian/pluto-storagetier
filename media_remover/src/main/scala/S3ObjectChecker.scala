import org.slf4j.LoggerFactory
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.Try

class S3ObjectChecker(client: S3AsyncClient, var bucketName: String)(implicit ec:ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  import S3ObjectChecker._

  def objectExistsWithSizeAndMaybeChecksum(objectKey: String, fileSize: Long, maybeLocalMd5: Option[String]): Future[Boolean] = {
    logger.info(s"Checking for existing versions of s3://$bucketName/$objectKey with size $fileSize and nearline checksum $maybeLocalMd5")
    val req = ListObjectVersionsRequest.builder().bucket(bucketName).prefix(objectKey).build()

    client.listObjectVersions(req).asScala.map(listObjectVersionsResponse => {
      val versions = listObjectVersionsResponse.versions().asScala
      logger.info(s"s3://$bucketName/$objectKey has ${versions.length} versions")
      versions.foreach(v => logger.info(s"s3://$bucketName/$objectKey @${v.versionId()} with size ${v.size()}, ETag ${v.eTag()} and checksum algorithm ${v.checksumAlgorithmAsStrings()} (has checksum alg: ${v.hasChecksumAlgorithm})"))
      val matchesForSize = versions.filter(_.size() == fileSize)
      if (matchesForSize.isEmpty) {
        logger.info(s"Found no entries for s3://$bucketName/$objectKey with size $fileSize, do require new copy")
        false
      } else {
        /* If we have a nearline MD5, and
           we have ETags that are MD5s
           then, the nearline MD5 must match one of the ETags.
           If no ETags are simple MD5s, we skip this check and are satisfied by the fact that the objectKey and size match.
         */
        maybeLocalMd5 match {
          case Some(localMd5) =>
            val matchesWithMd5 = matchesForSize.filter(m => eTagIsProbablyMd5(m.eTag))
            if (matchesWithMd5.isEmpty) {
              true
            } else {
              matchesWithMd5.count(_.eTag().equals(localMd5)) match { // TODO Check if both checksums are encoded the same way (hex, yes?)
                case 0 => false
                case _ => true
              }
            }
          case None =>
            logger.info(s"Found ${matchesForSize.length} existing entries for s3://$bucketName/$objectKey with size $fileSize, safe to delete locally")
            true
        }
      }
    }).recover({
      case _:NoSuchKeyException => false
      case err:Throwable =>
        logger.error(s"Could not check pre-existing versions for s3://$bucketName/$objectKey: ${err.getMessage}", err)
        throw new RuntimeException(s"Could not check pre-existing versions for s3://$bucketName/$objectKey: ${err.getMessage}")
     })
  }
}

object S3ObjectChecker {

  private def initS3AsyncClient = wrapJavaMethod(()=>{
    val b = S3AsyncClient.builder().httpClientBuilder(NettyNioAsyncHttpClient.builder())

    val withRegion = sys.env.get("AWS_REGION") match {
      case Some(rgn)=>b.region(Region.of(rgn))
      case None=>b
    }
    withRegion.build()
  })

  private def wrapJavaMethod[A](blk: ()=>A) = Try { blk() }.toEither.left.map(_.getMessage)

  def createFromEnvVars(bucketNameVar:String)(implicit ec:ExecutionContext):Either[String, S3ObjectChecker] =
    sys.env.get(bucketNameVar) match {
      case Some(mediaBucket) =>
        for {
          s3AsyncClient <- initS3AsyncClient
          result <- Right(new S3ObjectChecker(s3AsyncClient, mediaBucket))
        } yield result
      case None =>
        Left(s"You must specify $bucketNameVar so we know where uploads are!")
    }

  def eTagIsProbablyMd5(m: String): Boolean = {
    m.length == 32 && m.forall(c => "abcdefABCDEF0123456789".contains(c))
  }
}
