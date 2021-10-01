package utils

import io.circe.{Decoder, Encoder}

import java.nio.charset.StandardCharsets

object ArchiveHunter {
  val maxIdLength=512
  private final val encoder = java.util.Base64.getEncoder

  object ProxyType extends Enumeration {
    val VIDEO, AUDIO, THUMBNAIL, METADATA, UNKNOWN = Value
  }
  type ProxyType = ProxyType.Value

  object ProxyTypeEncoder {
    implicit val proxyTypeEncoder = Encoder.encodeEnumeration(ProxyType)
    implicit val proxyTypeDecoder = Decoder.decodeEnumeration(ProxyType)
  }

  case class ImportProxyRequest(itemId: String, proxyPath: String, proxyBucket:Option[String], proxyType: ProxyType)

  private def truncateId(initialString:String, chunkLength:Int):String = {
    /* I figure that the best way to get something that should be unique for a long path is to chop out the middle */
    val stringParts = initialString.grouped(chunkLength).toList
    val midSectionLength = maxIdLength - chunkLength*2
    if(midSectionLength>0) {
      val finalString = stringParts.head + stringParts(1).substring(0, midSectionLength) + stringParts(2)
      encoder.encodeToString(finalString.getBytes(StandardCharsets.UTF_8))
    } else if(chunkLength*2<maxIdLength) {  //maxIdLength < chunkLength-2
      val finalString = stringParts.head + stringParts(2)
      encoder.encodeToString(finalString.getBytes(StandardCharsets.UTF_8))
    } else {
      encoder.encodeToString(stringParts(2).getBytes(StandardCharsets.UTF_8))
    }
  }

  def makeDocId(bucket: String, key:String):String = {
    val initialString = bucket + ":" + key
    if(initialString.length<=maxIdLength){
      encoder.encodeToString(initialString.toCharArray.map(_.toByte))
    } else {
      truncateId(initialString, initialString.length/3)
    }
  }
}
