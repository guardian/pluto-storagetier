package helpers

import java.io.{File, FileReader}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.stream.{ClosedShape, Materializer, SinkShape}
import akka.stream.scaladsl.{FileIO, Framing, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import io.circe.Json
import models.{Incoming, IncomingFilename, IncomingListEntry}
import org.slf4j.LoggerFactory
import streamcomponents.{MarshalJsonToIncoming, ParseJsonEntry}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


object ListReader {
  private val logger = LoggerFactory.getLogger(getClass)
  val maxLinesize = 8192

  /**
    * reads a newline-delimited file list from an HTTP URL. The final list is buffered in memory and returned as a Seq[String].
    * this is called by ListReader.read() to handle any path starting with 'http:'
    * @param uri URI to access
    * @param acceptContentType optional string, if set tell HTTP server that this is the content type we want
    * @param system implicitly provided actor system
    * @param mat implicitly provided materializer
    * @param ec implicitly provided execution context
    * @return a Future containing either a Right with a sequence of strings if the read was successful or a Left with the
    *         content body as a UTF-8 string if the server did not return a 200 Success result. 201 No Content is treated as an error, because we need
    *         some content to work with
    */
  def fromHttp(uri:Uri,acceptContentType:Option[String])(implicit system:ActorSystem, mat:Materializer, ec:ExecutionContext) = {
    val headers:scala.collection.immutable.Seq[HttpHeader] = scala.collection.immutable.Seq(
      acceptContentType.map(ct=>new Accept(scala.collection.immutable.Seq(MediaRange(MediaType.custom(ct,false)))))
    ).collect({case Some(hdr)=>hdr})

    Http().singleRequest(HttpRequest(HttpMethods.GET,uri,headers)).flatMap(response=>{
      response.entity.withoutSizeLimit().dataBytes
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = maxLinesize))
        .map(_.decodeString("UTF-8"))
        .toMat(Sink.fold[Seq[Incoming],String](Seq())((acc,entry)=>acc++Seq(IncomingFilename(entry))))(Keep.right)
        .run()
        .map(result=>{
          if(response.status!=StatusCodes.OK){
            Right(result)
          } else {
            Left(result.mkString("\n"))
          }
        })
    })
  }

  /**
    * returns an akka stream sink to decode an incoming stream of newline-delimited JSON
    * this needs to be hooked into a ByteString producing source, presumably from Akka http
    */
  def ndJsonDecodeSink = {
    val sinkFactory = Sink.fold[Seq[Incoming],IncomingListEntry](Seq())((acc,entry)=>acc++Seq(entry))

    GraphDSL.create(sinkFactory) { implicit builder => sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val framer = builder.add(Framing.delimiter(ByteString("\n"), maximumFrameLength = maxLinesize).map(_.decodeString("UTF-8")))
      val parser = builder.add(new ParseJsonEntry().async)
      val marshaller = builder.add(new MarshalJsonToIncoming().async)
      framer ~> parser ~> marshaller ~> sink
      SinkShape(framer.in)
    }
  }

  def foldStringSink = {
    Sink.reduce[ByteString]((acc, entry)=>acc.concat(entry))
  }

  /**
    * receives a list of files to process as newline-delimited JSON from an HTTP(S) endpoint
    * @param uri URI to receive from
    * @param acceptContentType Optional content type to insist on from the server
    * @param system implicitly provided Actor System
    * @param mat implicitly provided Materializer
    * @param ec implicitly provided Execution Context
    * @return a Future, containing :
    *         if the response is not 200 (Success), a Left with the content body as a UTF-8 string
    *         if the response is 200 (Success), a Right with decoded IncomingListEntry objects.
    */
  def fromHttpNDJson(uri:Uri,acceptContentType:Option[String])(implicit system:ActorSystem, mat:Materializer, ec:ExecutionContext) = {
    val headers:scala.collection.immutable.Seq[HttpHeader] = scala.collection.immutable.Seq(
      acceptContentType.map(ct=>new Accept(scala.collection.immutable.Seq(MediaRange(MediaType.custom(ct,false)))))
    ).collect({case Some(hdr)=>hdr})

    Http().singleRequest(HttpRequest(HttpMethods.GET,uri,headers)).flatMap(response=>{
      if(response.status==StatusCodes.OK){
        response.entity.withoutSizeLimit().dataBytes.toMat(ndJsonDecodeSink)(Keep.right).run().map(Right(_))
      } else {
        response.entity.withoutSizeLimit().dataBytes.toMat(foldStringSink)(Keep.right).run().map(bodyBytes=>{
          val bodyString = bodyBytes.decodeString("UTF-8")
          Left(bodyString)
        })
      }
    })
  }

  /**
    * reads a newline-delimited file list from a local file.he final list is buffered in memory and returned as a Seq[String].
    * this is called by ListReader.read() if there is no obvious protocol handler.
    *
    * @param file
    * @param mat
    * @return
    */
  def fromFile(file:File)(implicit mat:Materializer,ec:ExecutionContext) = {
    try {
      FileIO.fromPath(file.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = maxLinesize))
        .map(_.decodeString("UTF-8"))
        .toMat(Sink.fold[Seq[Incoming], String](Seq())((acc, entry) => acc ++ Seq(IncomingFilename(entry))))(Keep.right)
        .run()
        .map(result=>Right(result))
        .recover({
          case err:Throwable=>
            Left(err.toString)
        })
    } catch {
      case err:Throwable=>
        Future(Left(err.toString))
    }
  }

  def fromFileNDJson(file:File)(implicit mat:Materializer,ec:ExecutionContext) = {
    try {
      FileIO.fromPath(file.toPath)
        .toMat(ndJsonDecodeSink)(Keep.right)
        .run()
        .map(result=>Right(result))
        .recover({
          case err:Throwable=>
            Left(err.toString)
        })
    } catch {
      case err:Throwable=>
        Future(Left(err.toString))
    }
  }

  /**
    * reads a file list from some path. Anythin starting with http: or https: is downloaded via akka http, otherwise
    * it's treated as a local file
    * @param path path to download
    * @param acceptContentType Optional string to stipulate content type when downloading from a server
    * @param system
    * @param mat
    * @param ec
    * @return a Future, with either an error message in a Left or a sequence of filenames in a Right
    */
  def read(path:String, acceptContentType:Option[String]=None, withJson:Boolean=false)(implicit system:ActorSystem, mat:Materializer, ec:ExecutionContext) = {
    if(path.startsWith("http:") || path.startsWith("https:")){
      if(withJson) {
        fromHttpNDJson(Uri(path + "?json=true"), acceptContentType)
      } else {
        fromHttp(Uri(path), acceptContentType)
      }
    } else {
      if(withJson) {
        fromFileNDJson(new File(path))
      } else {
        fromFile(new File(path))
      }
    }
  }
}