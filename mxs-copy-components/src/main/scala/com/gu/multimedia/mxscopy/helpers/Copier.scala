package com.gu.multimedia.mxscopy.helpers

import akka.NotUsed
import akka.actor.ActorSystem

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.Instant
import akka.stream.{ClosedShape, Materializer, SourceShape}
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.stage.GraphStage
import akka.util.ByteString
import com.om.mxs.client.japi.{Attribute, MatrixStore, MxsObject, UserInfo, Vault}
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import org.slf4j.{LoggerFactory, MDC}
import com.gu.multimedia.mxscopy.streamcomponents.{ChecksumSink, MMappedFileSource, MatrixStoreFileSink, MatrixStoreFileSource}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import org.apache.commons.io.FilenameUtils

object Copier {
  private val logger = LoggerFactory.getLogger(getClass)


  /**
   * composable function that performs a streaming copy from one objectmatrix entity to another
   * @param sourceFactory GraphStage object that will provide the raw file content
   * @param destFile MxsObject representing the file to be streamed to
   * @return a Future, containing a Long of the number of bytes written
   */
  private def streamBytes(source: Source[ByteString, NotUsed], destFile:MxsObject)(implicit mat:Materializer, ec:ExecutionContext) = {
    val sinkFac = new MatrixStoreFileSink(destFile)
    val streamBytesGraph = GraphDSL.createGraph(sinkFac) { implicit builder=> sink=>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val src = builder.add(source)
      src ~> sink
      ClosedShape
    }

    val debugContext = MDC.getCopyOfContextMap  //save the logger's debug context and make sure it is re-applied at the end.
    RunnableGraph
      .fromGraph(streamBytesGraph)
      .run()
      .map(result=>{
        MDC.setContextMap(debugContext)
        result
      })
  }

  /**
   * composable function that checks if the given checksums match
   * @param sourceDestChecksum a sequence of exactly two Try[String] representing the checksum operations.
   * @return a failed Future if either of the checksums is a Failure or if they are both successful but do not match.
   *         Otherwise, a successful Future containing the valid checksum
   */
  def validateChecksum(sourceDestChecksum:Seq[Try[String]], destFile:MxsObject)(implicit ec:ExecutionContext) = {
    val failures = sourceDestChecksum.collect({case Failure(err)=>err})
    if(failures.nonEmpty) {
      logger.error(s"${failures.length} checksums from the appliance failed:")
      failures.foreach(err=>logger.error(s"\t${err.getMessage}"))
      destFile.delete() //delete the target file because we could not validate it. Copy will be attempted again on the next retry.
      Future.failed(new RuntimeException("Could not validate checksums"))
    } else {
      val successes = sourceDestChecksum.collect({case Success(cs)=>cs})
      if(successes.head != successes(1)) {
        logger.warn(s"Appliance copy failed: Source checksum was ${successes.head} and destination checksum was ${successes(1)}")
        destFile.delete() //delete the target file because we could not validate it. Copy will be attempted again on the next retry.
        Future.failed(new RuntimeException("Checksums did not match"))
      } else {
        Future(successes.head)
      }
    }
  }

  /**
   * performs a safe copy from the given byte stream
   * @param sourceStream source of ByteStrings representing data to be streamed into the file
   * @param destVault vault on which to create the file
   * @param destMeta a sequence of Attribute values to get written onto the file
   * @param mat implicitly provided materializer
   * @param ec implicitly provided execution context
   * @return a Future, containing a tuple with the created objectmatrix ID and the validated checksum.
   *         On any error (including the checksum validation failing) then the future fails.
   */
  def doStreamCopy(sourceStream:Source[ByteString, Any], destVault:Vault, destMeta:Array[Attribute])(implicit mat:Materializer, ec:ExecutionContext) = {
    def option2try[A](src:Option[A], errMsg:String):Try[A] = src match {
      case None=>
        Failure(new RuntimeException(errMsg))
      case Some(value)=>Success(value)
    }

    Try { destVault.createObject(destMeta) } match {
      case Failure(err)=>
        logger.error(s"Could not create an object on vault ${destVault.getId}: ${err.getMessage}")
        logger.error(s"Object metadata was: ${destMeta.map(attr=>s"${attr.getKey}: ${attr.getValue}").mkString("; ")}")
        Future.failed(new RuntimeException("Could not create output file, see logs for details"))
      case Success(destObject) =>
        val checksumSink = new ChecksumSink("md5")
        val graph = GraphDSL.createGraph(checksumSink) { implicit builder => checksummer =>
          import akka.stream.scaladsl.GraphDSL.Implicits._

          val splitter = builder.add(Broadcast[ByteString](2, eagerCancel = true))
          val writer = builder.add(new MatrixStoreFileSink(destObject))
          sourceStream ~> splitter ~> writer
          splitter.out(1) ~> checksummer
          ClosedShape
        }

        for {
          maybeChecksum <- RunnableGraph.fromGraph(graph).run()
          destinationChecksum <- MatrixStoreHelper.getOMFileMd5(destObject)
          validatedChecksum <- validateChecksum(Seq(option2try(maybeChecksum, "No source checksum was received"), destinationChecksum), destObject)
        } yield (destObject.getId, Some(validatedChecksum))
    }
  }
  /**
   * stream a file from one ObjectMatrix vault to another
   * @param sourceVault Vault instance giving the vault to copy _from_
   * @param source [[ObjectMatrixEntry]] instance giving the source file to copy
   * @param destVault Vault instance giving the vault to copy _to_
   * @param keepOnFailure
   * @param retryOnFailure
   * @param actorSystem
   * @param ec
   * @param mat
   */
  def doCrossCopy(sourceVault:Vault, sourceOID:String, destVault:Vault, keepOnFailure:Boolean=false, retryOnFailure:Boolean=true)
                 (implicit actorSystem:ActorSystem, ec:ExecutionContext,mat:Materializer):Future[(String,Option[String])] = {

    for {
      sourceObject <- Future.fromTry(Try { sourceVault.getObject(sourceOID)})
      sourceMetadata <- MetadataHelper.getAttributeMetadata(sourceObject)
      destFile <- Future.fromTry(Try { destVault.createObject(sourceMetadata.toAttributes.toArray)} )
      writtenLength <- streamBytes(MatrixStoreFileSource(sourceVault, sourceOID), destFile)
      sourceDestChecksum <- Future.sequence(Seq(
        MatrixStoreHelper.getOMFileMd5(sourceObject),
        MatrixStoreHelper.getOMFileMd5(destFile)
      ))
      validatedChecksum <- validateChecksum(sourceDestChecksum, destFile) //this will fail the Future if the checksums don't match
    } yield (destFile.getId, Some(validatedChecksum))

  }

  /**
    * stream a file from the local filesystem into objectmatrix, creating metadata from what is provided by the filesystem.
    * also, performs a checksum on the data as it is copied and sets this in the object's metadata too.
    * @param vault `vault` object indicating where the file is to be stored
    * @param destFileName destination file name. this is checked beforehand, if it exists then no new file will be copied
    * @param fromFile java.nio.File indicating the file to copy from
    * @param chunkSize chunk size when streaming the file.
    * @param checksumType checksum type. This must be one of the digest IDs supported by java MessageDigest.
    * @param keepOnFailure boolean, if true then even if a checksum does not match the destination file is kept.
    *                      Defaults to false, delete destination file if checksum does not match.
    * @param retryOnFailure boolean, if true then try again if the checksum does not match. Defaults to true
    * @param ec implicitly provided execution context
    * @param mat implicitly provided materializer
    * @return a Future, with a tuple of (object ID, checksum)
    */
  def doCopyTo(vault:Vault, destFileName:Option[String], fromFile:File, chunkSize:Int, checksumType:String, keepOnFailure:Boolean=false,retryOnFailure:Boolean=true)(implicit ec:ExecutionContext,mat:Materializer):Future[(String,Option[String])] = {
    val checksumSinkFactory = checksumType match {
      case "none"=>Sink.ignore.mapMaterializedValue(_=>Future(None))
      case _=>new ChecksumSink(checksumType).async
    }
    val metadata = MatrixStoreHelper.metadataFromFilesystem(fromFile)

    if(metadata.isFailure){
      logger.error(s"Could no lookup metadata")
      Future.failed(metadata.failed.get) //since the stream future fails on error, might as well do the same here.
    } else {
      try {
        val mdToWrite = destFileName match {
          case None =>
            logger.info(s"MXFS_PATH (from filesystem) is ${fromFile.getAbsolutePath}")
            metadata.get
              .withString("MXFS_PATH",fromFile.getAbsolutePath)
              .withString("MXFS_FILENAME", fromFile.getName)
              .withString("MXFS_FILENAME_UPPER", fromFile.getName.toUpperCase)
          case Some(fn) =>
            val p = Paths.get(fn)
            val filenameOnly = p.getFileName.toString
            logger.info(s"MXFS_PATH (modified) is $fn")
            metadata.get
              .withString("MXFS_PATH", fn)
              .withString("MXFS_FILENAME", filenameOnly)
              .withString("MXFS_FILENAME_UPPER", filenameOnly.toUpperCase)
        }
        val timestampStart = Instant.now.toEpochMilli

        val mxsFile = vault.createObject(mdToWrite.toAttributes.toArray)

        logger.debug(s"mxsFile is $mxsFile")
        val graph = GraphDSL.createGraph(checksumSinkFactory) { implicit builder =>
          checksumSink =>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            //val src = builder.add(new MMappedFileSource(fromFile, chunkSize))
            val src = builder.add(FileIO.fromPath(fromFile.toPath))
            val bcast = builder.add(new Broadcast[ByteString](2, true))
            val omSink = builder.add(new MatrixStoreFileSink(mxsFile).async)

            src.out.log("copyToStream") ~> bcast ~> omSink
            bcast.out(1) ~> checksumSink
            ClosedShape
        }
        logger.debug(s"Created stream")
        RunnableGraph.fromGraph(graph).run().flatMap(finalChecksum=>{
          val timestampFinish = Instant.now.toEpochMilli
          val msDuration = timestampFinish - timestampStart

          val rate = fromFile.length().toDouble / msDuration.toDouble //in bytes/ms
          val mbps = rate /1048576 *1000  //in MByte/s

          logger.info(s"Stream completed, transferred ${fromFile.length} bytes in $msDuration millisec, at a rate of $mbps mByte/s.  Final checksum is $finalChecksum")
          finalChecksum match {
            case Some(actualChecksum)=>
              val updatedMetadata = metadata.get.copy(stringValues = metadata.get.stringValues ++ Map(checksumType->actualChecksum))
              MetadataHelper.setAttributeMetadata(mxsFile, updatedMetadata)

              MatrixStoreHelper.getOMFileMd5(mxsFile).flatMap({
                case Failure(err)=>
                  logger.error(s"Unable to get checksum from appliance, file should be considered unsafe", err)
                  Future.failed(err)
                case Success(remoteChecksum)=>
                  logger.info(s"Appliance reported checksum of $remoteChecksum")
                  if(remoteChecksum!=actualChecksum){
                    logger.error(s"Checksum did not match!")
                    if(!keepOnFailure) {
                      logger.info(s"Deleting invalid file ${mxsFile.getId}")
                      mxsFile.deleteForcefully()
                    }
                    if(retryOnFailure){
                      Thread.sleep(500)
                      doCopyTo(vault, destFileName, fromFile, chunkSize, checksumType, keepOnFailure, retryOnFailure)
                    } else {
                      Future.failed(new RuntimeException(s"Checksum did not match"))
                    }
                  } else {
                    Future((mxsFile.getId, finalChecksum))
                  }
              })
            case _=>
              Future((mxsFile.getId, finalChecksum))
          }
        })
      } catch {
        case err:Throwable=>
          logger.error(s"Could not prepare copy: ", err)
          Future.failed(err)
      }
    }
  }


  def ensurePathExists(pathName:String) = {
    val pathPart = new File(FilenameUtils.getPathNoEndSeparator(pathName))
    logger.info(s"creating directories $pathPart")
    pathPart.mkdirs()
  }

  /**
    * returns true if the file does not exist or is zero-length, and should be overwritten
    * @param filePath path to check
    * @return bolean
    */
  def isAbsentOrZerolength(filePath:String) = {
    val f = new File(filePath)
    !f.exists() && f.length()==0
  }

  /**
    * removes a leading slash from a filepath, if present
    * @param from
    * @return
    */
  def removeLeadingSlash(from:String) = {
    if(from.startsWith("/")){
      from.substring(1)
    } else {
      from
    }
  }
}
