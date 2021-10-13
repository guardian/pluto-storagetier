package archivehunter

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.GenericHttpCredentials
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.stream.Materializer
import com.gu.multimedia.storagetier.utils.AkkaHttpHelpers.{RedirectRequired, RetryRequired, consumeStream, contentBodyToJson}
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.{ExecutionContext, Future}
import io.circe.generic.auto._
import io.circe.syntax._
import com.gu.multimedia.storagetier.utils.AkkaHttpHelpers
import utils.ArchiveHunter

import java.security.MessageDigest

class ArchiveHunterCommunicator(config:ArchiveHunterConfig) (implicit ec:ExecutionContext, mat:Materializer, actorSystem:ActorSystem){
  private final val logger = LoggerFactory.getLogger(getClass)

  import ArchiveHunterResponses._

  final val httpDateFormatter = DateTimeFormatter.RFC_1123_DATE_TIME

  protected def callHttp = Http()

  /**
   * generates a signature string for the given URI and time
   * @param uri URI to sign
   * @param formattedTime String of the date/time in RFC 1123 format
   * @return a String of the base64-encoded digest for ArchiveHunter
   */
  protected def getToken(uri:Uri, formattedTime:String, contentLength:Int,
                       requestMethod:String,
                       contentChecksum:String) = {
    val stringtoSign = s"$formattedTime\n$contentLength\n$contentChecksum\n$requestMethod\n${uri.path.toString()}"

    logger.debug(s"stringToSign: $stringtoSign")

    val secret = new SecretKeySpec(config.sharedSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA384")
    val mac = Mac.getInstance("HmacSHA384")
    mac.init(secret)
    val hashBytes = mac.doFinal(stringtoSign.getBytes(StandardCharsets.UTF_8))
    Base64.getEncoder.encodeToString(hashBytes)
  }

  /**
   * Makes an authenticated request to Archive Hunter and unmarshals the returned JSON into the given type.
   * Will retry on 5xx errors up to 10 times.  Redirects are followed up to a depth of 10.
   * All errors (including unmarshalling errors) result in a failed Future
   * @param req HttpRequest to make
   * @param maybeChecksum If you are passing a content body, include the SHA-384 checksum here encoded as a base64 string. Otherwise specify None.
   * @param attempt internally used attempt counter, don't specify this. if > 10 then the request is not made.
   * @param overrideTime override the "current" time used when calculating the signature. Used for testing.
   * @tparam T type to unmarshal returned JSON into
   * @return a Future, containing the unmarshalled data if the server returned 200 or None if the server returned 404. All other
   *         response codes result in a failed Future
   */
  protected def callToArchiveHunter[T:io.circe.Decoder](req: HttpRequest, maybeChecksum:Option[String], attempt: Int = 1, retryLimit:Int=10, overrideTime:Option[ZonedDateTime]=None): Future[Option[T]] = if (retryLimit > 10) {
    Future.failed(new RuntimeException("Too many retries, see logs for details"))
  } else {
    logger.debug(s"Request URL is ${req.uri.toString()}")

    val contentChecksum = maybeChecksum match {
      case Some(content)=>content
      case None=>     //if we are not sending any content, use the checksum of an empty string
        val checksumBytes = MessageDigest.getInstance("SHA-384").digest("".getBytes)
        Base64.getEncoder.encodeToString(checksumBytes)
    }
    val signatureTime = overrideTime.getOrElse(ZonedDateTime.now()).withZoneSameInstant(ZoneId.of("UTC"))
    val httpDate = httpDateFormatter.format(signatureTime)
    val token = getToken(req.uri, httpDate, req.entity.contentLengthOption.getOrElse(0L).toInt, req.method.value, contentChecksum)

    val updatedRequest = req.withHeaders(req.headers ++ Seq(
      akka.http.scaladsl.model.headers.RawHeader("Date", httpDate),
      akka.http.scaladsl.model.headers.RawHeader("X-Sha384-Checksum", contentChecksum),
      akka.http.scaladsl.model.headers.Authorization(GenericHttpCredentials("HMAC", token))
    ))

    callHttp
      .singleRequest(updatedRequest)
      .flatMap(response=>{
        AkkaHttpHelpers.handleResponse(response, "Archive Hunter").flatMap({
          case Right(Some(stream))=>
            contentBodyToJson(consumeStream(stream.dataBytes))
          case Right(None)=>
            Future(None)
          case Left(RedirectRequired(newUri))=>
              logger.info(s"Redirecting to $newUri")
              callToArchiveHunter(req.withUri(newUri), maybeChecksum, attempt + 1)
          case Left(RetryRequired)=>
              Thread.sleep(500 * attempt)
              callToArchiveHunter(req, maybeChecksum, attempt + 1)
        })
      })
  }

  /**
   * finds the given docId in ArchiveHunter and verifies that it does, indeed, point to the given bucket and path.
   * If the ID does not exist, then False is returned in the Future.
   * If the ID does exist, then True is returned in the Future providing that it points to the given bucket and path.
   * If it does not point to the right place, then the Future is failed.
   * @param docId ArchiveHunter ID to search for
   * @param uploadedBucket bucket where we uploaded the content
   * @param uploadedPath path on the bucket where we uploaded the content
   * @return a Future containing a boolean flag.  If the future fails then treat it as a permanant error and do not re-try
   */
  def lookupArchivehunterId(docId:String, uploadedBucket:String, uploadedPath:String) = {
    val req = HttpRequest(uri=s"${config.baseUri}/api/entry/$docId")
    callToArchiveHunter[ArchiveHunterEntryResponse](req, None).flatMap({
      case None=>
        Future(false)    //the ID does not exist
      case Some(response)=>  //hooray, the ID does exist
        if(response.objectClass!="entry") {
          Future.failed(new RuntimeException(s"Expected an entity type of 'entry' but ArchiveHunter gave us '${response.objectClass}'!"))
        } else if(response.entry.path!=uploadedPath || response.entry.bucket!=uploadedBucket) {
          Future.failed(new RuntimeException(s"The ID links to another file, we expected $uploadedBucket:$uploadedPath but got ${response.entry.bucket}:${response.entry.path}"))
        } else {
          logger.info(s"Found s3://${response.entry.bucket}/${response.entry.path} with storage class ${response.entry.storageClass} and last-modified time ${response.entry.last_modified}")
          Future(true)
        }
    })
  }

  def importProxy(docId:String, proxyPath:String, proxyBucket:String, proxyType:ArchiveHunter.ProxyType) = {
    import utils.ArchiveHunter.ProxyTypeEncoder._
    import akka.http.scaladsl.model.headers._
    import akka.http.scaladsl.model.ContentTypes
    val requestContent = ArchiveHunter.ImportProxyRequest(docId, proxyPath, Some(proxyBucket), proxyType).asJson.noSpaces
    val requestBody = HttpEntity(requestContent).withContentType(ContentTypes.`application/json`)

    logger.debug(s"URI is ${config.baseUri}/api/importProxy, content is $requestContent")
    val req = HttpRequest(uri=s"${config.baseUri}/api/importProxy",method = HttpMethods.POST, entity=requestBody)

    val checksumBytes = MessageDigest.getInstance("SHA-384").digest(requestContent.getBytes)
    val contentChecksum = Base64.getEncoder.encodeToString(checksumBytes)

    callToArchiveHunter[Map[String,String]](req, Some(contentChecksum)).flatMap({
      case None=>
        Future.failed(new RuntimeException("The item ID was not found"))
      case Some(_)=>
        Future( () )
    }).recoverWith({
      case err:RuntimeException=>
        if(err.getMessage.contains("conflict error")) { //ignore conflict errors, if we get one then we have a proxy.
          Future( () )
        } else {
          Future.failed(err)
        }
    })
  }
}
