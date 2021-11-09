import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
import com.gu.multimedia.mxscopy.helpers.{Copier, MatrixStoreHelper, MetadataHelper}
import com.gu.multimedia.mxscopy.streamcomponents.ChecksumSink
import com.om.mxs.client.japi.{MxsObject, Vault}
import org.slf4j.{LoggerFactory, MDC}

import java.nio.file.{Files, Path}
import java.util.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class FileCopier()(implicit ec:ExecutionContext, mat:Materializer) {
  private val logger = LoggerFactory.getLogger(getClass)

  protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path) = {
    val fromFile = filePath.toFile

    Copier.doCopyTo(vault, Some(fileName), fromFile, 2*1024*1024, "md5").map(value => Right(value._1))
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
