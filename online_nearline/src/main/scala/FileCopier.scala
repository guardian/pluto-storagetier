import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import com.gu.multimedia.mxscopy.helpers.{Copier, MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.models.ObjectMatrixEntry
import com.gu.multimedia.mxscopy.streamcomponents.ChecksumSink
import com.om.mxs.client.japi.{MxsObject, Vault}
import org.slf4j.{LoggerFactory, MDC}

import java.nio.file.{Files, Path}
import java.util.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class FileCopier()(implicit ec:ExecutionContext, mat:Materializer) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val numberedMatcherWithExt = "-(\\d+)\\.[^.]+$".r.unanchored
  private val numberedMatcherNoExt = "-(\\d+)$".r.unanchored
  private val fileSplitter = "^(.*)\\.([^\\.]+)$".r

  /**
   * Internal method. This takes an ObjectMatrixEntry from a search and tries to find a "-(number)" in its filepath.
   * If so, it's returned in an option. If nothing is found then None is returned.
   * It's assumed that the relevant fields are present in the ObjectMAtrixEntry, otherwise None will always be returned
   * @param elem ObjectMatrixEntry to check
   * @return the index portion of the filename or None if it was not present
   */
  protected def maybeGetIndex(elem:ObjectMatrixEntry) = elem.pathOrFilename match {
    case Some(filePath)=>
      filePath match {
        case numberedMatcherWithExt(num)=>Some(num.toInt)
        case numberedMatcherNoExt(num)=>Some(num.toInt)
        case _=>Some(0)
      }
    case None=>
      None
  }

  protected def callFindByFilenameNew(vault:Vault, fileName:String) = MatrixStoreHelper.findByFilenameNew(vault, fileName)

  /**
   * Find a free filename in the form file-{n}.xxx or file-{n}.
   * Note that this is deprecated as it messes with the natural way of managing versions on the matrixstore.
   * @param vault vault to check
   * @param fileName initial filename to check
   * @return
   */
  @deprecated
  def updateFilenameIfRequired(vault:Vault, fileName:String) = callFindByFilenameNew(vault, fileName)
    .map(objects=>{
      if(objects.isEmpty) {
        logger.debug(s"Found no other files matching $fileName so continuing with that name")
        fileName
      } else {
        logger.debug(s"Found ${objects.length} files matching $fileName:")
        objects.foreach(ent=>logger.debug(s"\t${ent.pathOrFilename}"))
        if(objects.length>100) {
          throw new BailOutException("Debugging error, not expecting more than 100 matches. Check the filtering functions.")
        } else {
          //find the highest number that is used as a filename index and add one
          val numberToUse = objects.foldLeft(0)((acc,elem)=>{
            maybeGetIndex(elem) match {
              case Some(num)=>if(num>=acc) num+1 else acc
              case None=>acc
            }
          })
          fileName match {
            case fileSplitter(path, xtn)=>
              path + s"-$numberToUse" + "." + xtn
            case _=>
              fileName + s"-$numberToUse"
          }
        }
      }
    })

  /**
   * Searches the given vault for files matching the given specification.
   * Both the name and fileSize must match in order to be considered valid.
   * @param vault vault to search
   * @param filePath Path representing the file path to look for.
   * @param fileSize Long representing the size of the file to match
   * @return a Future, containing a sequence of ObjectMatrixEntries that match the given file path and size
   */
  def findMatchingFilesOnNearline(vault:Vault, filePath:Path, fileSize:Long) = {
    logger.debug(s"Looking for files matching $filePath at size $fileSize")
    callFindByFilenameNew(vault, filePath.toString)
      .map(fileNameMatches=>{
        val nullSizes = fileNameMatches.map(_.maybeGetSize()).collect({case None=>None}).length
        if(nullSizes>0) {
          throw new BailOutException(s"Could not check for matching files of $filePath because $nullSizes / ${fileNameMatches.length} had no size")
        }

        val sizeMatches = fileNameMatches.filter(_.maybeGetSize().contains(fileSize))
        logger.debug(s"$filePath: ${fileNameMatches.length} files matched name and ${sizeMatches.length} matched size")
        logger.debug(fileNameMatches.map(obj=>s"${obj.pathOrFilename.getOrElse("-")}: ${obj.maybeGetSize()}").mkString("; "))
        sizeMatches
      })
  }

  protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path) = {
    val fromFile = filePath.toFile
    Copier.doCopyTo(vault, Some(fileName), fromFile, 2*1024*1024, "md5").map(value => Right(value._1))
  }

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

  protected def getSizeFromPath(filePath: Path) = {
    Files.size(filePath)
  }

  protected def getSizeFromMxs(mxsFile: MxsObject) = {
    MetadataHelper.getFileSize(mxsFile)
  }

  /**
   * Checks to see if any of the MXS files in the `potentialFiles` list are a checksum match for `filePath`.
   * Stops and returns the ID of the first match if it finds one, or returns None if there were no matches.
   * @param filePath local file that is being backed up
   * @param potentialFiles potential backup copies of this file
   * @param maybeLocalChecksum stored local checksum; if set this is used instead of re-calculating. Leave this out when calling.
   * @return a Future containing the OID of the first matching file if present or None otherwise
   */
  protected def verifyChecksumMatch(filePath:Path, potentialFiles:Seq[MxsObject], maybeLocalChecksum:Option[String]=None):Future[Option[String]] = potentialFiles.headOption match {
    case None=>
      logger.info(s"$filePath: No matches found for file checksum")
      Future(None)
    case Some(mxsFileToCheck)=>
      logger.info(s"$filePath: Verifying checksum for MXS file ${mxsFileToCheck.getId}")
      val savedContext = getContextMap()  //need to save the debug context for when we go in and out of akka
      val requiredChecksums = Seq(
        maybeLocalChecksum.map(cs=>Future(Some(cs))).getOrElse(getChecksumFromPath(filePath)),
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

  protected def openMxsObject(vault:Vault, oid:String) = Try { vault.getObject(oid) }

  /**
   * Copies the given file from the filesystem to MXS.
   * If the given file already exists in the MXS vault (i.e., there is a file with a matching MXFS_PATH _and_ checksum
   * _and_ file size, then a Right (copy-success) is returned immediately with no copy performed.
   * If there is a file with matching MXFS_PATH but checksum and/or size do not match, then a new version is created.
   * If there is a failure while copying, a temporary failure is returned in a Left
   * @param vault Vault to copy to
   * @param fileName file name to use on the Vault
   * @param filePath java.nio.Path giving the filepath of the item to back up
   * */
  def copyFileToMatrixStore(vault: Vault, fileName: String, filePath: Path): Future[Either[String, String]] = {
    ( for {
      fileSize <- Future.fromTry(Try { getSizeFromPath(filePath) })
      potentialMatches <- findMatchingFilesOnNearline(vault, filePath, fileSize)
      potentialMatchesFiles <- Future.sequence(potentialMatches.map(entry=>Future.fromTry(openMxsObject(vault, entry.oid))))
      alreadyExists <- verifyChecksumMatch(filePath, potentialMatchesFiles)
      result <- alreadyExists match {
        case Some(existingId)=>
          logger.info(s"$filePath: Object already exists with object id ${existingId}")
          Future(Right(existingId))
        case None=>
          logger.info(s"$filePath: Out of ${potentialMatches.length} remote matches, none matched the checksum so creating new copy")
          copyUsingHelper(vault, fileName, filePath)
      }
    } yield result )
      .recoverWith({
        case err:java.io.IOException =>
          if(err.getMessage.contains("does not exist (error 306)")) {
            copyUsingHelper(vault, fileName, filePath)
          } else {
            //the most likely cause of this is that the sdk threw because the appliance is under heavy load and
            //can't do the checksum at this time
            logger.error(s"Error validating objectmatrix checksum: ${err.getMessage}", err)
            Future(Left(s"ObjectMatrix error: ${err.getMessage}"))
          }
        case err:BailOutException=>
          logger.error(s"A permanent error occurred: ${err.getMessage}", err)
          Future.failed(err)
        case err:Throwable =>
          // Error contacting ObjectMatrix, log it and retry via the queue
          logger.warn(s"Failed to get object from vault for checksum $filePath: ${err.getClass.getCanonicalName} ${err.getMessage} , will retry", err)
          Future(Left(s"ObjectMatrix error: ${err.getMessage}"))
        case _ =>
          Future(Left(s"ObjectMatrix error"))
      })
  }

}
