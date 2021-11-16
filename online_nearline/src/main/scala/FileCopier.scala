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
          throw new Exception("Debugging error, not expecting more than 100 matches. Check the filtering functions.")
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
      .map(_.filter(_.maybeGetSize().contains(fileSize)))
  }

  protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path) = {
    val fromFile = filePath.toFile

    updateFilenameIfRequired(vault, filePath.toString)
      .flatMap(nameToUse=>{
        Copier.doCopyTo(vault, Some(nameToUse), fromFile, 2*1024*1024, "md5").map(value => Right(value._1))
      }).recover({
        case err:Throwable=>
          logger.error(s"Unexpected error occurred when trying to determine filename to use: ${err.getMessage}")
          Left(err.getMessage)
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

  def copyFileToMatrixStore(vault: Vault, fileName: String, filePath: Path, objectId: Option[String]): Future[Either[String, String]] = {
    objectId match {
      case Some(id) =>
        Future.fromTry(Try { vault.getObject(id) })
          .flatMap({ mxsFile =>
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
                val localFileSize = getSizeFromPath(filePath)

                if (getSizeFromMxs(mxsFile) == localFileSize) { //file size and checksums match, no copy required
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
            case err:Throwable =>
              // Error contacting ObjectMatrix, log it and retry via the queue
              logger.info(s"Failed to get object from vault $filePath: ${err.getMessage} for checksum, will retry")
              Future(Left(s"ObjectMatrix error: ${err.getMessage}"))
            case _ =>
              Future(Left(s"ObjectMatrix error"))
          })
      case None =>
        copyUsingHelper(vault, fileName, filePath)
    }
  }
}
