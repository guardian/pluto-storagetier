package com.gu.multimedia.mxscopy.helpers

import java.nio.ByteBuffer
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.om.mxs.client.japi.{AttributeView, MxsObject, ObjectTypedAttributeView}
import com.gu.multimedia.mxscopy.models.MxsMetadata
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object MetadataHelper {
  private val logger = LoggerFactory.getLogger(getClass)
  /**
    * iterates the available metadata and presents it as a dictionary
    * @param obj [[MxsObject]] entity to retrieve information from
    * @param mat implicitly provided materializer for streams
    * @param ec implicitly provided execution context
    * @return a Future, with the relevant map
    */
  def getAttributeMetadata(obj:MxsObject)(implicit mat:Materializer, ec:ExecutionContext) = {
    val view = obj.getAttributeView

    //val sink = Sink.fold[Seq[(String,AnyRef)],(String,AnyRef)](Seq())((acc,elem)=>acc++Seq(elem))
    val sink = Sink.fold[MxsMetadata,(String,Any)](MxsMetadata(Map(),Map(),Map(),Map()))((acc,elem)=>{
      elem._2 match {
        case boolValue: Boolean => acc.copy(boolValues = acc.boolValues ++ Map(elem._1->boolValue))
        case intValue:Int => acc.copy(intValues = acc.intValues ++ Map(elem._1 -> intValue))
        case longValue:Long => acc.copy(longValues = acc.longValues ++ Map(elem._1 -> longValue))
        case byteBuffer:ByteBuffer => acc.copy(stringValues = acc.stringValues ++ Map(elem._1 -> Hex.encodeHexString(byteBuffer.array())))
        case stringValue:String => acc.copy(stringValues = acc.stringValues ++ Map(elem._1 -> stringValue))
        case _=>
          logger.warn(s"Could not get metadata value for ${elem._1} on ${obj.getId}, type ${elem._2.getClass.toString} not recognised")
          acc
      }
    })
    Source.fromIterator(()=>view.iterator.asScala)
      .map(elem=>(elem.getKey, elem.getValue))
      .toMat(sink)(Keep.right)
      .run()
      //.map()
  }

  /**
    * get the MXFS file metadata
    * @param obj [[MxsObject]] entity to retrieve information from
    * @return
    */
  def getMxfsMetadata(obj:MxsObject) = {
    val view = obj.getMXFSFileAttributeView
    view.readAttributes()
  }

  def setAttributeMetadata(obj:MxsObject, newMetadata:MxsMetadata) = {
    val view = obj.getAttributeView

    //meh, this is probably not very efficient
    newMetadata.stringValues.foreach(entry=>view.writeString(entry._1,entry._2))
    newMetadata.longValues.foreach(entry=>view.writeLong(entry._1, entry._2))
    newMetadata.intValues.foreach(entry=>view.writeInt(entry._1,entry._2))
    newMetadata.boolValues.foreach(entry=>view.writeBoolean(entry._1, entry._2))
  }

  def safeReadLong(view:ObjectTypedAttributeView, key:String):Option[Long] = {
    Try { Option(view.readLong(key)) }.toOption.flatten
  }
  def safeReadString(view:ObjectTypedAttributeView, key:String):Option[String] = {
    Try { Option(view.readString(key)) }.toOption.flatten
  }

  /**
   * exhaustive determination of file size.  Tries __mxs__length and DPSP_SIZE (both as a Long and a String) from the metadata first.
   * If nothing is found, then falls back to using MxfsAttributeView
   * @param obj
   * @return
   */
  def getFileSize(obj:MxsObject) = {
    val view = obj.getAttributeView
    val metaValue = (safeReadLong(view, "__mxs__length"), safeReadLong(view, "DPSP_SIZE"), safeReadString(view, "DPSP_SIZE")) match {
      case (Some(size), _, _) => //__mxs__length _should_ always be set
        logger.debug(s"getting size of ${obj.getId} from __mxs__length")
        Some(size)
      case (_, Some(size), _) => //DPSP_SIZE is set by Dropspot and some GNM tools
        logger.debug(s"getting size of ${obj.getId} from DPSP_SIZE (long)")
        Some(size)
      case (_, _, Some(sizeString)) => //one of our tools mischaracterised DPSP_SIZE as a string
        logger.debug(s"getting size of ${obj.getId} from DPSP_SIZE (string)")
        Try {
          sizeString.toLong
        } match {
          case Success(size) => Some(size)
          case Failure(err) =>
            logger.error(s"Invalid DPSP_SIZE value '$sizeString' on ${obj.getId} could not be converted from string to number: ${err.getMessage}")
            None
        }
      case (_, _, _) =>
        None
    }

    metaValue match {
      case Some(size)=>size
      case None=>
        //MXFSAttributeView seems unreliable for some reason
        logger.debug(s"Could not find any file size for ${obj.getId} in metadata, falling back to MXFSAttributeView")
        val mxfsMeta = obj.getMXFSFileAttributeView
        val attrs = mxfsMeta.readAttributes()
        attrs.size()
    }
  }
}
