package archivehunter

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.stream.Materializer
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.concurrent.{ExecutionContext, Future}
import io.circe.generic.auto._

class ArchiveHunterCommunicator(config:ArchiveHunterConfig) (implicit ec:ExecutionContext, mat:Materializer, actorSystem:ActorSystem){
  private final val logger = LoggerFactory.getLogger(getClass)
  import utils.AkkaHttpHelpers._
  import ArchiveHunterResponses._

  private val httpDateFormatter = DateTimeFormatter.RFC_1123_DATE_TIME

  protected def callHttp = Http()

  /**
   * generates a signature string for the given URI and time
   * @param uri URI to sign
   * @param formattedTime String of the date/time in RFC 1123 format
   * @return a String of the base64-encoded digest for ArchiveHunter
   */
  private def getToken(uri:Uri, formattedTime:String) = {
    val stringtoSign = s"$formattedTime\n${uri.path.toString()}"
    logger.debug(s"stringToSign: $stringtoSign")

    val secret = new SecretKeySpec(config.sharedSecret.getBytes(StandardCharsets.UTF_8), "SHA256")
    val mac = Mac.getInstance("SHA256")
    mac.init(secret)
    val hashBytes = mac.doFinal(stringtoSign.getBytes(StandardCharsets.UTF_8))
    Base64.getEncoder.encodeToString(hashBytes)
  }

  /**
   * Makes an authenticated request to Archive Hunter and unmarshals the returned JSON into the given type.
   * Will retry on 5xx errors up to 10 times.  Redirects are followed up to a depth of 10.
   * All errors (including unmarshalling errors) result in a failed Future
   * @param req HttpRequest to make
   * @param attempt internally used attempt counter, don't specify this. if > 10 then the request is not made.
   * @param overrideTime override the "current" time used when calculating the signature. Used for testing.
   * @tparam T type to unmarshal returned JSON into
   * @return a Future, containing the unmarshalled data if the server returned 200 or None if the server returned 404. All other
   *         response codes result in a failed Future
   */
  protected def callToArchiveHunter[T:io.circe.Decoder](req: HttpRequest, attempt: Int = 1, retryLimit:Int=10, overrideTime:Option[ZonedDateTime]=None): Future[Option[T]] = if (retryLimit > 10) {
    Future.failed(new RuntimeException("Too many retries, see logs for details"))
  } else {
    logger.debug(s"Request URL is ${req.uri.toString()}")

    val signatureTime = overrideTime.getOrElse(ZonedDateTime.now()).withZoneSameInstant(ZoneId.of("UTC"))
    val httpDate = httpDateFormatter.format(signatureTime)
    val token = getToken(req.uri, httpDate)

    val updatedRequest = req.withHeaders(req.headers ++ Seq(
      akka.http.scaladsl.model.headers.RawHeader("X-Gu-Tools-HMAC-Token", token),
      akka.http.scaladsl.model.headers.RawHeader("X-Gu-Tools-HMAC-Date", httpDate)
    ))

    callHttp
      .singleRequest(updatedRequest)
      .flatMap(response=>{
        val contentBody = consumeResponseEntity(response.entity)

        response.status.intValue() match {
          case 200 =>
            contentBodyToJson(contentBody)
          case 404 =>
            Future(None)
          case 403 =>
            throw new RuntimeException(s"Archive Hunter said permission denied.") //throwing an exception here will fail the future,
          //which is picked up in onComplete in the call
          case 400 =>
            contentBody.map(body => throw new RuntimeException(s"Archive Hunter returned bad data error: $body"))
          case 301 =>
            logger.warn(s"Received unexpected redirect from pluto to ${response.getHeader("Location")}")
            val h = response.getHeader("Location")
            if (h.isPresent) {
              val newUri = h.get()
              logger.info(s"Redirecting to ${newUri.value()}")
              val updatedReq = req.withUri(Uri(newUri.value()))
              callToArchiveHunter(updatedReq, attempt + 1)
            } else {
              throw new RuntimeException("Unexpected redirect without location")
            }
          case 500 | 502 | 503 | 504 =>
            contentBody.flatMap(body => {
              logger.error(s"Pluto returned a server error: $body. Retrying...")
              Thread.sleep(500 * attempt)
              callToArchiveHunter(req, attempt + 1)
            })
        }
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
   * @return a Future containing a boolean flag.
   */
  def lookupArchivehunterId(docId:String, uploadedBucket:String, uploadedPath:String) = {
    val req = HttpRequest(uri=s"${config.baseUri}/api/entry/$docId")
    callToArchiveHunter[ArchiveHunterEntryResponse](req).map({
      case None=>
        false    //the ID does not exist
      case Some(response)=>  //hooray, the ID does exist
        if(response.entityType!="entry") {
          throw new RuntimeException(s"Expected an entity type of 'entry' but ArchiveHunter gave us '${response.entityType}'!")
        } else if(response.entity.path!=uploadedPath || response.entity.bucket!=uploadedBucket) {
          throw new RuntimeException(s"The ID links to another file, we expected $uploadedBucket:$uploadedPath but got ${response.entity.bucket}:${response.entity.path}")
        } else {
          logger.info(s"Found s3://${response.entity.bucket}/${response.entity.path} with storage class ${response.entity.storageClass} and last-modified time ${response.entity.last_modified}")
          true
        }
    })
  }
}
