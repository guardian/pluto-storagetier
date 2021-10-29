package com.gu.multimedia.mxscopy.helpers

import akka.actor.ActorSystem

import java.io.File
import java.nio.file.Path
import java.time.Instant
import akka.stream.{ClosedShape, Materializer}
import akka.stream.scaladsl.{Broadcast, FileIO, GraphDSL, RunnableGraph, Sink}
import akka.util.ByteString
import com.om.mxs.client.japi.{MatrixStore, MxsObject, UserInfo, Vault}
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import org.slf4j.{LoggerFactory, MDC}
import com.gu.multimedia.mxscopy.streamcomponents.{ChecksumSink, MMappedFileSource, MatrixStoreFileSink, MatrixStoreFileSource}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import org.apache.commons.io.FilenameUtils

object Copier {
  private val logger = LoggerFactory.getLogger(getClass)

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
  def doCrossCopy(sourceVault:Vault, source:ObjectMatrixEntry, destVault:Vault, keepOnFailure:Boolean=false, retryOnFailure:Boolean=true)
                 (implicit actorSystem:ActorSystem, ec:ExecutionContext,mat:Materializer):Future[(String,Option[String])] = {

    /**
     * composable function that performs a streaming copy from one objectmatrix entity to another
     * @param destFile MxsObject representing the file to be streamed to
     * @return a Future, containing a Long of the number of bytes written
     */
    def streamBytes(destFile:MxsObject) = {
      val sinkFac = new MatrixStoreFileSink(destFile)
      val streamBytesGraph = GraphDSL.createGraph(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(new MatrixStoreFileSource(sourceVault, source.oid))
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
    def validateChecksum(sourceDestChecksum:Seq[Try[String]]) = {
      val failures = sourceDestChecksum.collect({case Failure(err)=>err})
      if(failures.nonEmpty) {
        logger.error(s"${failures.length} checksums from the appliance failed:")
        failures.foreach(err=>logger.error(s"\t${err.getMessage}"))
        Future.failed(new RuntimeException("Could not validate checksums"))
      } else {
        val successes = sourceDestChecksum.collect({case Success(cs)=>cs})
        if(successes.head != successes(1)) {
          logger.warn(s"Appliance copy failed: Source checksum was ${successes.head} and destination checksum was ${successes(1)}")
          Future.failed(new RuntimeException("Checksums did not match"))
        } else {
          Future(successes.head)
        }
      }
    }

    for {
      sourceMetadata <- MetadataHelper.getAttributeMetadata(source.getMxsObject(sourceVault))
      destFile <- Future.fromTry(Try { destVault.createObject(sourceMetadata.toAttributes.toArray)} )
      writtenLength <- streamBytes(destFile)
      sourceDestChecksum <- Future.sequence(Seq(
        MatrixStoreHelper.getOMFileMd5(source.getMxsObject(sourceVault)),
        MatrixStoreHelper.getOMFileMd5(destFile)
      ))
      validatedChecksum <- validateChecksum(sourceDestChecksum) //this will fail the Future if the checksums don't match
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
          case Some(fn) => metadata.get
            .withString("MXFS_PATH",fromFile.getAbsolutePath)
            .withString("MXFS_FILENAME", fromFile.getName)
            .withString("MXFS_FILENAME_UPPER", fromFile.getName.toUpperCase)
          case None => metadata.get.withValue[Int]("dmmyInt",0)
        }
        val timestampStart = Instant.now.toEpochMilli

        logger.debug(s"mdToWrite is $mdToWrite")
        logger.debug(s"attributes are ${mdToWrite.toAttributes.map(_.toString).mkString(",")}")
        val mxsFile = vault.createObject(mdToWrite.toAttributes.toArray)

        logger.debug(s"mxsFile is $mxsFile")
        val graph = GraphDSL.create(checksumSinkFactory) { implicit builder =>
          checksumSink =>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val src = builder.add(new MMappedFileSource(fromFile, chunkSize))
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
