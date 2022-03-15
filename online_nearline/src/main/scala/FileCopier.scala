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
        val sizeMatches = fileNameMatches.filter(_.maybeGetSize().contains(fileSize))
        logger.debug(s"$filePath: ${fileNameMatches.length} files matched name and ${sizeMatches.length} matched size")
        logger.debug(fileNameMatches.map(obj=>s"${obj.pathOrFilename.getOrElse("-")}: ${obj.maybeGetSize()}").mkString("; "))
        sizeMatches
      })
  }

  protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path) = {
    val fromFile = filePath.toFile

    updateFilenameIfRequired(vault, filePath.toString)
      .flatMap(nameToUse=>{
        Copier.doCopyTo(vault, Some(nameToUse), fromFile, 2*1024*1024, "md5").map(value => Right(value._1))
      })
  }

  protected def getContextMap() = {
    MDC.getCopyOfContextMap
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
   * Copies the given file from the filesystem to MXS.
   * If the given file already exists in the MXS vault (i.e., there is a file with a matching MXFS_PATH _and_ checksum
   * _and_ file size, then a Right (copy-success) is returned immediately with no copy performed.
   * If there is a file with matching MXFS_PATH but checksum and/or size do not match, then a new version is created.
   * If there is a failure while copying, a temporary failure is returned in a Left
   * @param vault MXS vault to check and copy to
   * @param fileName name of the file to be copied
   * @param filePath filesystem path to the file to be copied (as java.nio.Path)
   * @param objectId existing objectId of the (possibly previous) version
   * @return a Future that completes when the operation is done, containing either a Right for success (with the MXS ID in it)
   *         or a Left on error (with an error string in it)
   */
  def copyFileToMatrixStore(vault: Vault, fileName: String, filePath: Path, objectId: Option[String]): Future[Either[String, String]] = {
    objectId match {
      case Some(id) =>
        Future.fromTry(Try { vault.getObject(id) })
          .flatMap({ mxsFile =>
            val localFileSize = getSizeFromPath(filePath)
            //get the file size first, as it's possible for our connection to be logged out by the time the checksum finishes
            val remoteFileSize = getSizeFromMxs(mxsFile)

            // File exist in ObjectMatrix check size and md5
            val savedContext = getContextMap  //need to save the debug context for when we go in and out of akka
            val checksumMatchFut = Future.sequence(Seq(
              getChecksumFromPath(filePath),
              getOMFileMd5(mxsFile)
            )).map(results => {
              setContextMap(savedContext)
              val fileChecksum      = results.head.asInstanceOf[Option[String]]
              val applianceChecksum = results(1).asInstanceOf[Try[String]]
              logger.debug(s"fileChecksum is $fileChecksum")
              logger.debug(s"applianceChecksum is $applianceChecksum")
              fileChecksum == applianceChecksum.toOption
            })

            checksumMatchFut.flatMap({
              case true => //checksums match
                setContextMap(savedContext)

                if (remoteFileSize == localFileSize) { //file size and checksums match, no copy required
                  logger.info(s"Object with object id ${id} and filepath ${filePath} already exists")
                  Future(Right(id))
                } else { //checksum matches but size does not (unlikely but possible), new copy required
                  logger.info(s"Object with object id ${id} and filepath $filePath exists but size does not match, copying" +
                    s" fresh version")
                  copyUsingHelper(vault, fileName, filePath)
                }
              case false =>
                setContextMap(savedContext)
                //checksums don't match, size match undetermined, new copy required
                logger.info(s"Object with object id ${id} and filepath $filePath exists but checksum does not match, copying fresh " +
                  s"version")
                copyUsingHelper(vault, fileName, filePath)
            })
          }).recoverWith({
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
      case None =>
        copyUsingHelper(vault, fileName, filePath)
    }
  }
}
