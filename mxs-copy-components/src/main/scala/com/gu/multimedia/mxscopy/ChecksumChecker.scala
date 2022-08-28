package com.gu.multimedia.mxscopy

import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import com.gu.multimedia.mxscopy.streamcomponents.ChecksumSink
import com.om.mxs.client.japi.MxsObject
import org.slf4j.{LoggerFactory, MDC}

import java.nio.file.Path
import java.util.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class ChecksumChecker()(implicit ec:ExecutionContext, mat:Materializer)
{
  private val logger = LoggerFactory.getLogger(getClass)

  protected def getContextMap() = {
    Option(MDC.getCopyOfContextMap)
  }

  protected def setContextMap(contextMap: Map[String, String]) = {
    MDC.setContextMap(contextMap)
  }

  protected def getOMFileMd5(mxsFile: MxsObject) = {
    MatrixStoreHelper.getOMFileMd5(mxsFile)
  }

  protected def getChecksumFromPath(filePath: Path) = {
    FileIO.fromPath(filePath).runWith(ChecksumSink.apply)
  }

  /**
   * Checks to see if any of the MXS files in the `potentialFiles` list are a checksum match for `filePath`.
   * Stops and returns the ID of the first match if it finds one, or returns None if there were no matches.
   * @param filePath local file that is being backed up
   * @param potentialFiles potential backup copies of this file
   * @param maybeLocalChecksum stored local checksum; if set this is used instead of re-calculating. Leave this out when calling.
   * @return a Future containing the OID of the first matching file if present or None otherwise
   */
  def verifyChecksumMatch(filePath: Path,
                          potentialFiles: Seq[MxsObject],
                          maybeLocalChecksum: Option[String]=None): Future[Option[String]] =
    potentialFiles.headOption match {
    case None=>
      logger.info(s"$filePath: No matches found for file checksum")
      Future(None)
    case Some(mxsFileToCheck)=>
      logger.info(s"$filePath: Verifying checksum for MXS file ${mxsFileToCheck.getId}")
      val savedContext = getContextMap()  //need to save the debug context for when we go in and out of akka
      val requiredChecksums = Seq(
        maybeLocalChecksum.map(checksum=>Future(Some(checksum))).getOrElse(getChecksumFromPath(filePath)),
        getOMFileMd5(mxsFileToCheck)
      )
      Future
        .sequence(requiredChecksums)
        .map(results => {
          if(savedContext.isDefined) setContextMap(savedContext.get)
          val fileChecksum      = results.head.asInstanceOf[Option[String]]
          val applianceChecksum = results(1).asInstanceOf[Try[String]]
          logger.info(s"$filePath: local checksum is $fileChecksum, ${mxsFileToCheck.getId} checksum is $applianceChecksum")
          (fileChecksum == applianceChecksum.toOption, fileChecksum)
        })
        .flatMap({
          case (true, _)=>
            logger.info(s"$filePath: Got a checksum match for remote file ${mxsFileToCheck.getId}")
            Future(Some(potentialFiles.head.getId))  //true => we got a match
          case (false, localChecksum)=>
            logger.info(s"$filePath: ${mxsFileToCheck.getId} did not match, trying the next entry of ${potentialFiles.tail.length}")
            verifyChecksumMatch(filePath, potentialFiles.tail, localChecksum)
        })
  }
}
