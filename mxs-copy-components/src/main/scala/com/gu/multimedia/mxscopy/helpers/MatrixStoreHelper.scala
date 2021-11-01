package com.gu.multimedia.mxscopy.helpers

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{Files, LinkOption}
import java.nio.file.attribute.{BasicFileAttributes, FileTime}
import java.time.temporal.TemporalField
import java.time.{Instant, ZoneId, ZonedDateTime}
import akka.stream.{ClosedShape, Materializer, SourceShape}
import akka.stream.scaladsl.{GraphDSL, Keep, RunnableGraph, Sink, Source}
import com.om.mxs.client.internal.TaggedIOException
import com.om.mxs.client.japi.{Constants, MatrixStore, MxsObject, SearchTerm, UserInfo, Vault}
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import org.slf4j.LoggerFactory
import com.gu.multimedia.mxscopy.streamcomponents.{OMLookupMetadata, OMSearchSource}
import org.apache.commons.codec.binary.Hex

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

object MatrixStoreHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  def openVault(userInfo:UserInfo):Try[Vault] = Try {
    MatrixStore.openVault(userInfo)
  }

  private val escaper = """([+\-&|!(){}\[\]^"~*?:\\/])""".r

  def escapeForQuery(unEscapedString:String) = {
    escaper.replaceAllIn(unEscapedString, matched=>{
      "\\\\" + matched.group(1)
    })
  }

  def buildRangeQueryString(forPath:String, onField:String) = {
    val escapedPath = escapeForQuery(forPath)
    val separator = "\u241D"

    Seq(s"range:$onField", "string", ">=", escapedPath, "<=", escapedPath).mkString(separator)
  }

  /**
    * locate files for the given filename, as stored in the metadata. This assumes that one or at most two records will
    * be returned and should therefore be more efficient than using the streaming interface. If many records are expected,
    * this will be inefficient and you should use the streaming interface
    * @param vault MXS `vault` object
    * @param fileName file name to search for
    * @return a Try, containing either a sequence of zero or more results as [[ObjectMatrixEntry]] records or an error
    */
  def findByFilename(vault:Vault, fileName:String, onField:String="MXFS_FILENAME"):Try[Seq[ObjectMatrixEntry]] = Try {
    val escapedFileName = escapeForQuery(fileName)
    logger.debug(s"checking for $escapedFileName in $onField")
    def doSearch(searchTerm:SearchTerm) = {
      val iterator = vault.searchObjectsIterator(searchTerm, 1).asScala

      var finalSeq: Seq[ObjectMatrixEntry] = Seq()
      while (iterator.hasNext) {
        finalSeq ++= Seq(ObjectMatrixEntry(iterator.next(), None, None))
      }
      finalSeq
    }

    val regularSearchResults = doSearch(SearchTerm.createSimpleTerm(onField, fileName))

    val oidList = if(regularSearchResults.nonEmpty) {
      s"results - ${regularSearchResults.map(_.oid).mkString(",")}"
    } else {
      ""
    }

    logger.info(s"$fileName on $onField: ${regularSearchResults.length} $oidList")
    regularSearchResults
  }

  /**
    * helper function to initialise a Source that finds elements matching the given name and looks up their metadata.
    * both of these operations are performed with async barriers
    * @param userInfo MXS UserInfo object that provides cluster, login and vault details
    * @param searchTerms search terms to search for, as MXS SearchTerms object
    * @param mat implicitly provided actor materializer
    * @param ec implicitly provided execution context
    * @return a partial graph that provides a Source to be mixed into another stream
    */
  def findBulkSource(userInfo:UserInfo, searchTerms:SearchTerm)(implicit mat:Materializer, ec:ExecutionContext) = {
    GraphDSL.create() { implicit builder=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(new OMSearchSource(userInfo,Some(searchTerms),None).async)
      val mdLookup = builder.add(new OMLookupMetadata(userInfo).async)

      src ~> mdLookup

      SourceShape(mdLookup.out)
    }
  }

  /**
    * helper function to perform a filename search using the streaming interface
    * @param userInfo  MXS UserInfo object that provides cluster, login and vault details
    * @param fileName file name to search for
    * @param mat implicitly provided actor materializer
    * @param ec implicitly provided execution context
    * @return a Future, containing a Sequence of matching [[ObjectMatrixEntry]] records. If the stream fails then
    *         the future is cancelled; use either .onComplete or .recover/.recoverWith to handle this.
    */
  def findByFilenameBulk(userInfo:UserInfo, fileName:String)(implicit mat:Materializer, ec:ExecutionContext) = {
    val sinkFactory = Sink.fold[Seq[ObjectMatrixEntry],ObjectMatrixEntry](Seq())((acc,entry)=>acc ++ Seq(entry))
    val searchTerm = SearchTerm.createSimpleTerm("MXFS_FILENAME",fileName)

//    val graph = GraphDSL.create(sinkFactory) { implicit builder=> sink=>
//      import akka.stream.scaladsl.GraphDSL.Implicits._
//      val src = findBulkSource(userInfo, searchTerm)
//
//      src ~> sink
//      ClosedShape
//    }
//
//    RunnableGraph.fromGraph(graph).run()
    Source.fromGraph(findBulkSource(userInfo, searchTerm))
      .toMat(sinkFactory)(Keep.right)
      .run()
  }

  /**
    * returns the file extension of the provided filename, or None if there is no extension
    * @param fileNameString filename string
    * @return the content of the last extension
    */
  def getFileExt(fileNameString:String):Option[String] = {
    val re = ".*\\.([^\\.]+)$".r

    fileNameString match {
      case re(xtn) =>
        if (xtn.length < 8) {
          Some(xtn)
        } else {
          logger.warn(s"$xtn does not look like a file extension (too long), assuming no actual extension")
          None
        }
      case _ => None
    }
  }

  /**
    * converts mime type into a category integer, as per MatrixStoreAdministrationProgrammingGuidelines.pdf p.9
    * @param mt MIME type as string
    * @return an integer
    */
  val mimeTypeRegex = "^([^\\/]+)/(.*)$".r

  def categoryForMimetype(mt: Option[String]):Int = mt match {
    case None=>
      logger.warn(s"No MIME type provided!")
      0
    case Some(mimeTypeRegex("video",minor)) =>2
    case Some(mimeTypeRegex("audio",minor)) =>3
    case Some(mimeTypeRegex("document",minor)) =>4
    case Some(mimeTypeRegex("application",minor)) =>4
    case Some(mimeTypeRegex("image",minor))=>5
    case Some(mimeTypeRegex(major,minor))=>
      logger.info(s"Did not regognise major type $major (minor was $minor)")
      0
    case _=>
      logger.warn(s"invalid mimetype was given: ${mt.getOrElse("none")}")
      0
  }

   /** initialises an MxsMetadata object from filesystem metadata. Use when uploading files to matrixstore/
    * @param file java.io.File object to check
    * @return either an MxsMetadata object or an error
    */
  def metadataFromFilesystem(file:File):Try[MxsMetadata] = Try {
    val path = file.getAbsoluteFile.toPath
    val mimeType = Option(Files.probeContentType(file.toPath))

    val fsAttrs = Files.readAttributes(path,"*",LinkOption.NOFOLLOW_LINKS).asScala

    val maybeCtime = fsAttrs.get("creationTime").map(value=>ZonedDateTime.ofInstant(value.asInstanceOf[FileTime].toInstant,ZoneId.of("UTC")))
    val nowTime = ZonedDateTime.now()

    val uid = Files.getAttribute(path, "unix:uid", LinkOption.NOFOLLOW_LINKS).asInstanceOf[Int]
    MxsMetadata(
      stringValues = Map(
        "MXFS_FILENAME_UPPER" -> path.getFileName.toString.toUpperCase,
        "MXFS_FILENAME"->path.getFileName.toString,
        "MXFS_PATH"->path.toString,
        "MXFS_USERNAME"->uid.toString, //stored as a string for compatibility. There seems to be no easy way to look up the numeric UID in java/scala
        "MXFS_MIMETYPE"->mimeType.getOrElse("application/octet-stream"),
        "MXFS_DESCRIPTION"->s"File ${path.getFileName.toString}",
        "MXFS_PARENTOID"->"",
        "MXFS_FILEEXT"->getFileExt(path.getFileName.toString).getOrElse("")
      ),
      boolValues = Map(
        "MXFS_INTRASH"->false,
      ),
      longValues = Map(
        "DPSP_SIZE"->file.length(),
        "MXFS_MODIFICATION_TIME"->fsAttrs.get("lastModifiedTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
        "MXFS_CREATION_TIME"->fsAttrs.get("creationTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
        "MXFS_ACCESS_TIME"->fsAttrs.get("lastAccessTime").map(_.asInstanceOf[FileTime].toMillis).getOrElse(0),
      ),
      intValues = Map(
        "MXFS_CREATIONDAY"->maybeCtime.map(ctime=>ctime.getDayOfMonth).getOrElse(0),
        "MXFS_COMPATIBLE"->1,
        "MXFS_CREATIONMONTH"->maybeCtime.map(_.getMonthValue).getOrElse(0),
        "MXFS_CREATIONYEAR"->maybeCtime.map(_.getYear).getOrElse(0),
        "MXFS_CATEGORY"->categoryForMimetype(mimeType)
      )
    )
  }

  /** initialises an MxsMetadata object from filesystem metadata. Use when uploading files to matrixstore/
    * @param filepath filepath to check as a string. This is converted to a java.io.File and the other implementation is then called
    * @return either an MxsMetadata object or an error
    */
  def metadataFromFilesystem(filepath:String):Try[MxsMetadata] = metadataFromFilesystem(new File(filepath))

  /**
    * request MD5 checksum of the given object, as calculated by the appliance.
    * as per the MatrixStore documentation, a blank string implies that the digest is still being calculated; in this
    * case we sleep 1 second and try again.
    * for this reason we do the operation in a sub-thread
    * @param f MxsObject representing the object to checksum
    * @param ec implicitly provided execution context
    * @return a Future, which resolves to a Try containing a String of the checksum.
    */
  def getOMFileMd5(f:MxsObject, maxAttempts:Int=50)(implicit ec:ExecutionContext):Future[Try[String]] = {

    def lookup(attempt:Int=1):Try[String] = {
      if(attempt>maxAttempts) return Failure(new RuntimeException(s"Could not get valid checksum after $attempt tries"))
      logger.info(s"Requesting appliance-side MD5 checksum for ${f.getId} on attempt $attempt")
      val view = f.getAttributeView
      val result = Try {
        val buf = ByteBuffer.allocate(16)
        view.read("__mxs__calc_md5", buf)
        logger.debug(s"Appliance checksum request got buffer of length ${buf.array().length}")
        buf
      }

      result match {
        case Failure(err:TaggedIOException)=>
          if(err.getError==302){
            logger.warn(s"Got 302 (server busy) from appliance when requesting checksum, retrying after delay")
            Thread.sleep(30000*attempt) //we don't want to time out on a large file.Each retry takes 15s to time out, so add 30s per retry.
            lookup(attempt+1)
          } else {
            Failure(err)
          }
        case Failure(err:java.io.IOException)=>
          if(err.getMessage.contains("error 302")){
            logger.warn(s"Appliance side checksum request got an error containing 302 string, retrying after delay")
            Thread.sleep(30000*attempt)
            lookup(attempt+1)
          } else {
            Failure(err)
          }
        case Failure(otherError)=>Failure(otherError)
        case Success(buffer)=>
          val arr = buffer.array()
          if(arr.isEmpty) {
            logger.info(s"Empty string returned for file appliance-side checksum on attempt $attempt, assuming still calculating. Will retry...")
            Thread.sleep(30000*attempt) //this feels nasty but without resorting to actors i can't think of an elegant way
            //to delay and re-call in a non-blocking way
            lookup(attempt + 1)
          } else {
            logger.debug(s"Appliance-side checksum - byte string length was ${arr.length}")
            val converted = Hex.encodeHexString(arr)
            logger.debug(s"Appliance-side checksum - converted string was $converted")
            if (converted.length == 32)
              Success(converted)
            else {
              logger.warn(s"Returned appliance-side checksum $converted is wrong length (${converted.length}; should be 32).")
              Thread.sleep(1500)
              lookup(attempt + 1)
            }
          }
      }
    }

    Future { lookup() }
  }
}
