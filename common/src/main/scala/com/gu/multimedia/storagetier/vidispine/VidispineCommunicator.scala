package com.gu.multimedia.storagetier.vidispine

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, MediaRange, MediaRanges, MediaTypes}
import akka.http.scaladsl.model.headers.{Accept, Authorization, BasicHttpCredentials}
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source, StreamConverters}
import akka.util.ByteString
import org.slf4j.LoggerFactory
import io.circe.generic.auto._
import com.gu.multimedia.storagetier.utils.AkkaHttpHelpers
import com.gu.multimedia.storagetier.utils.AkkaHttpHelpers.{RedirectRequired, RetryRequired, consumeStream, contentBodyToJson}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import cats.implicits._

class VidispineCommunicator(config:VidispineConfig) (implicit ec:ExecutionContext, mat:Materializer, actorSystem:ActorSystem){
  private final val logger = LoggerFactory.getLogger(getClass)

  protected def callHttp = Http()

  /**
   * call out to Vidispine and return the content stream if successful. Use this for streaming raw content directly elsewhere
   * @param req HttpRequest to undertake, authorization is added to this
   * @param attempt attempt counter, don't specify this
   * @param retryLimit maximum number of retries
   * @return
   */
  protected def callToVidispineRaw(req: HttpRequest, attempt: Int = 1, retryLimit:Int=10):Future[Option[Source[ByteString, Any]]] = if (attempt > retryLimit) {
    Future.failed(new RuntimeException("Too many retries, see logs for details"))
  } else {
    logger.debug(s"Vidispine request URL is ${req.uri.toString()}")

    val updatedReq = req.withHeaders(req.headers ++ Seq(Authorization(BasicHttpCredentials(config.username, config.password))))

    callHttp
      .singleRequest(updatedReq)
      .flatMap(response=>AkkaHttpHelpers.handleResponse(response,"Vidispine"))
      .flatMap({
        case Right(Some(stream))=>Future(Some(stream))
        case Right(None)=>Future(None)
        case Left(RedirectRequired(newUri))=>
          logger.info(s"vidispine redirected to $newUri")
          callToVidispineRaw(req.withUri(newUri), attempt+1, retryLimit)
        case Left(RetryRequired)=>
          Thread.sleep(500*attempt)
          callToVidispineRaw(req, attempt+1, retryLimit)
      })
  }

  /**
   * more conventional `callTo` method which adds an "Accept: application/json" for Vidispine and then attempts
   * to decode the content using Circe to the given domain object. If the parsing fails, then the future will fail too.
   * @param req request to make. Authorization and Accept are both added
   * @param retryLimit maximum number of retries to do before failing
   * @tparam T data type to unmarshal returned JSON into
   * @return a Future, containing the data object or None if a 404 was returned. Other responses return an error.
   */
  protected def callToVidispine[T:io.circe.Decoder](req: HttpRequest, retryLimit:Int=10):Future[Option[T]] =
    callToVidispineRaw(
      req.withHeaders(req.headers :+ Accept(MediaRange(MediaTypes.`application/json`))),
      retryLimit = retryLimit
    ).flatMap({
      case None => Future(None)
      case Some(stream) =>
        contentBodyToJson(consumeStream(stream))
    })

  private def streamingVS(req:HttpRequest, readTimeout:FiniteDuration, thing:String) = callToVidispineRaw(req).map({
    case Some(stream)=>
      stream
        .toMat(StreamConverters.asInputStream(readTimeout))(Keep.right)
        .run()
    case None=>
      throw new RuntimeException(s"$thing does not exist")  //throwing here just causes a failed Future
  })

  /**
   * try to stream the content of the given file.  If the file exists, returns a Future which contains an InputStream
   * that allows the data to be directly streamed into the TransferManager library.
   * If the file does not exist (or another error occurs), then the Future fails
   * @param vsFileId file ID to stream
   * @param readTimeout Optional read timeout for the InputStream. Defaults to 5 seconds
   * @return a Future containing the InputStream
   */
  def streamFileContent(vsFileId:String, readTimeout:FiniteDuration=5.seconds) = {
    val req = HttpRequest(uri = s"${config.baseUri}/api/storage/file/$vsFileId/data")
    streamingVS(req, readTimeout, s"Vidispine file $vsFileId")
  }

  /**
   * try to stream the XML metadata of the given item.
   * @param itemId
   * @param readTimeout
   * @return
   */
  def streamXMLMetadataDocument(itemId:String, readTimeout:FiniteDuration=5.seconds) = {
    val headers = Seq(Accept(MediaRange(MediaTypes.`application/xml`)))
    val req = HttpRequest(uri = s"${config.baseUri}/api/item/$itemId/metadata", headers = headers)
    streamingVS(req, readTimeout, s"Vidispine item $itemId")
  }


  def getResourceUriList(itemId:String, itemVersion:Option[Int], resourceType: VidispineCommunicator.ResourceType.Value) = {
    val baseUriString = resourceType match {
      case VidispineCommunicator.ResourceType.Poster => s"${config.baseUri}/API/item/$itemId/posterresource"
      case VidispineCommunicator.ResourceType.Thumbnail => s"${config.baseUri}/API/item/$itemId/thumbnailresource"
    }
    val req = itemVersion match {
      case None=>HttpRequest(uri = baseUriString)
      case Some(version)=>HttpRequest(uri = baseUriString + s"?version=$version")
    }
    callToVidispine[UriListDocument](req)
  }

  protected def getThumbnailsList(maybeResourceUri:Option[String]) = maybeResourceUri.map(thumbnailResourceUri=>
      callToVidispine[UriListDocument](HttpRequest(uri = thumbnailResourceUri))
  ).sequence.map(_.flatten)

  protected def findFirstThumbnail(maybeResourceUri:Option[String], maybeThumbnailsList:Option[UriListDocument]) = {
    (maybeResourceUri, maybeThumbnailsList.flatMap(_.uri.headOption)) match {
      case (Some(resourceUri), Some(firstEntry))=>
        logger.debug(s"resource uri $resourceUri, first entry is $firstEntry")
        val targetUri = s"$resourceUri/$firstEntry"
        callToVidispineRaw(HttpRequest(uri=targetUri))
      case (_, _)=>Future(None)
    }
  }

  def akkaStreamFirstThumbnail(itemId:String, itemVersion:Option[Int]) = {
    for {
      maybeResourceUri <- getResourceUriList(itemId, itemVersion, VidispineCommunicator.ResourceType.Thumbnail).map(_.flatMap(_.uri.headOption))
      maybeThumbnailsList <- getThumbnailsList(maybeResourceUri)
      maybeStream <- findFirstThumbnail(maybeResourceUri, maybeThumbnailsList)
    } yield maybeStream
  }

  /**
   * tries to get an InputStream to obtain the data for the first thumbnail for the given item
   * If there is no data None is returned, otherwise an InputStream is returned
   * @param itemId
   * @param itemVersion
   * @return
   */
  def streamFirstThumbnail(itemId:String, itemVersion:Option[Int], readTimeout:FiniteDuration=5.seconds) = akkaStreamFirstThumbnail(itemId, itemVersion)
    .map(
      _.map(
        _.toMat(StreamConverters.asInputStream(readTimeout))(Keep.right).run()
      )
    )

  /**
   * tries to get an InputStream to obtain the data for the poster image for the given item.
   * If there is no data None is returned, otherwise InputStream is returned
   * @param itemId the item to get
   * @param itemVersion optional item version. Defaults to the latest one.
   * @param readTimeout optional read timeout for the InputStream. Defaults to 5 seconds
   * @return a Future, which contains either a connected InputStream or None.
   */
  def streamPosterForItem(itemId:String, itemVersion:Option[Int], readTimeout:FiniteDuration=5.seconds) = {
    getResourceUriList(itemId, itemVersion, VidispineCommunicator.ResourceType.Poster).flatMap({
      case None=>Future(None)
      case Some(uriList)=>
        val req = HttpRequest(uri = uriList.uri.head)
        streamingVS(req, readTimeout, s"Poster for vidispine item $itemId").map(Some.apply)
    })
  }
}

object VidispineCommunicator {
  object ResourceType extends Enumeration {
    val Poster, Thumbnail = Value
  }
}