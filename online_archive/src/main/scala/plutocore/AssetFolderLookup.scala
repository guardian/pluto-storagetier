package plutocore
import akka.actor.ActorSystem
import akka.dispatch.Dispatcher
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import com.gu.multimedia.storagetier.auth.HMAC
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.Future
import java.nio.file._
import scala.util.{Failure, Success, Try}
import io.circe.generic.auto._
import cats.implicits._

class AssetFolderLookup (config:PlutoCoreConfig)(implicit mat:Materializer, actorSystem:ActorSystem) {
  private implicit val dispatcher = actorSystem.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)

  import utils.AkkaHttpHelpers._

  /* extract call to static object to make testing easier */
  def callHttp = Http()

  private val multiSlashRemover = "^/{2,}".r

  /**
   * internal method that performs a call to pluto, handles response codes/retries and unmarshals reutrned JSON to a domain object.
   * If the server returns a 200 response then the content is parsed as JSON and unmarshalled into the given object
   * If the server returns a 404 response then None is returned
   * If the server returns a 403 or a 400 then a failed future is returned
   * If the server returns a 500, 502, 503 or 504 then the request is retried after (attempt*0.5) seconds up till 10 attempts
   *
   * @param req     constructed akka HttpRequest to perform
   * @param attempt attempt counter, you don't need to specify this when calling
   * @tparam T the type of domain object to unmarshal the response into. There must be an io.circe.Decoder in-scope for this kind of object.
   *           if the unmarshalling fails then a failed Future is returned
   * @return a Future containing an Option with either the unmarshalled domain object or None
   */
  protected def callToPluto[T: io.circe.Decoder](req: HttpRequest, attempt: Int = 1): Future[Option[T]] = if (attempt > 10) {
    Future.failed(new RuntimeException("Too many retries, see logs for details"))
  } else {
    logger.debug(s"Request URL is ${req.uri.toString()}")
    val checksumBytes = MessageDigest.getInstance("SHA-384").digest("".getBytes)
    val checksumString = checksumBytes.map("%02x".format(_)).mkString
    val queryPart = req.uri.rawQueryString.map(query => "?" + query).getOrElse("")
    val messageTime = ZonedDateTime.now()

    val contentType = if (req.entity.isKnownEmpty()) {
      ""
    } else {
      req.entity.contentType.mediaType.toString()
    }

    val token = HMAC.calculateHmac(
      contentType,
      checksumString,
      method = req.method.value,
      multiSlashRemover.replaceAllIn(req.uri.path.toString(), "/") + queryPart,
      config.sharedSecret,
      messageTime
    )

    if (token.isEmpty) {
      Future.failed(new RuntimeException("could not build authorization"))
    } else {

      val auth: HttpHeader = RawHeader("Authorization", s"HMAC ${token.get}")
      val checksum = RawHeader("Digest", s"SHA-384=$checksumString")
      val date = RawHeader("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(messageTime))
      val updatedReq = req.withHeaders(scala.collection.immutable.Seq(auth, date, checksum)) //add in the authorization header

      callHttp.singleRequest(updatedReq).flatMap(response => {
        val contentBody = consumeResponseEntity(response.entity)

        response.status.intValue() match {
          case 200 =>
            contentBodyToJson(contentBody)
          case 404 =>
            Future(None)
          case 403 =>
            throw new RuntimeException(s"Pluto said permission denied.") //throwing an exception here will fail the future,
          //which is picked up in onComplete in the call
          case 400 =>
            contentBody.map(body => throw new RuntimeException(s"Pluto returned bad data error: $body"))
          case 301 =>
            logger.warn(s"Received unexpected redirect from pluto to ${response.getHeader("Location")}")
            val h = response.getHeader("Location")
            if (h.isPresent) {
              val newUri = h.get()
              logger.info(s"Redirecting to ${newUri.value()}")
              val updatedReq = req.withUri(Uri(newUri.value()))
              callToPluto(updatedReq, attempt + 1)
            } else {
              throw new RuntimeException("Unexpected redirect without location")
            }
          case 500 | 502 | 503 | 504 =>
            contentBody.flatMap(body => {
              logger.error(s"Pluto returned a server error: $body. Retrying...")
              Thread.sleep(500 * attempt)
              callToPluto(req, attempt + 1)
            })
        }
      })
    }
  }

  /**
   * wraps the java Path.relativize call into a more scala-comprehension friendly form
   * @param path a Path representing the path to make relative to the asset folder root
   * @return a Right with the relative path.  If the requested path is not below the asset root or both paths are relative then the call will fail.
   */
  def relativizeFilePath(path:Path) =
    Try { config.assetFolderBasePath.relativize(path.toAbsolutePath.normalize()) } match {
      case Success(p)=>
        logger.debug(s"Relative file path is $p")
        if(p.startsWith("..")) {
          Left(s"${path.toString} is not below the asset folder root of ${config.assetFolderBasePath.toString}")
        } else {
          Right(p)
        }
      case Failure(err)=>
        Left(err.getMessage)
    }

  protected def findExpectedAssetfolder(path:Path) = {
    if(path.getNameCount>3) {
      Right(path.subpath(0, 3))
    } else {
      Left(s"There are not sufficient directories for $path to be an asset folder")
    }
  }

  protected def putBackBase(path:Path):Either[String, Path] = {
    Try { config.assetFolderBasePath.resolve(path) } match {
      case Success(p)=>Right(p)
      case Failure(err)=>Left(err.getMessage)
    }
  }

  /**
   * try to find the asset folder associated with the given file
   * @param forFile a Path representing the _file name_ (not directory!) to look up
   * @return a Future that contains the asset folder record, or None if there was nothing found. On error, the future will fail.
   */
  def assetFolderRecordLookup(forFile:Path) = {
    logger.debug(s"Finding asset folder for $forFile")

    val maybeAssetFolder = for {
      relativePath <- relativizeFilePath(forFile)
      assetFolder <- findExpectedAssetfolder(relativePath)
      fullPath <- putBackBase(assetFolder)
    } yield fullPath

    maybeAssetFolder match {
      case Left(err)=>
        logger.warn(err)
        Future(None)
      case Right(assetFolder)=>
        val req = HttpRequest(uri = s"${config.baseUri}/api/assetfolder/lookup?path=${URLEncoder.encode(assetFolder.toString, StandardCharsets.UTF_8)}")
        callToPluto[AssetFolderRecord](req)
    }
  }

  /**
   * try to find the project record associated with the given file.
   * Internally, this calles "assetFolderRecordLookup"
   * @param forFile a Path representing the _file name_ (not directory!) to look up
   * @return a Future that contains the project record, or None if there was nothing found. On error, the future will fail.
   */
  def assetFolderProjectLookup(forFile:Path) = {
    import ProjectRecordEncoder._

    assetFolderRecordLookup(forFile)
      .flatMap(
        _.map(record=>{
          val req = HttpRequest(uri = s"${config.baseUri}/api/project/${record.project}")
          callToPluto[ProjectRecord](req)
        }).sequence.map(_.flatten) //.sequence here is a bit of cats "magic" that turns the Option[Future[Option]] into a Future[Option[Option]]
      )
  }
}
