package com.gu.multimedia.storagetier.auth

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.security._
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

object HMAC {
  val logger = LoggerFactory.getLogger(this.getClass)

  private val httpDateFormatter = DateTimeFormatter.RFC_1123_DATE_TIME

  /**
   * generate the HMAC digest of a string, given a shared secret to encrypt it with
   * @param sharedSecret passphrase to encrypt with
   * @param preHashString content to digest
   * @return Base64 encoded string of the hmac digest
   */
  def generateHMAC(sharedSecret: String, preHashString: String): String = {
    val secret = new SecretKeySpec(sharedSecret.getBytes, "HmacSHA384")   //Crypto Funs : 'SHA256' , 'HmacSHA1'
    val mac = Mac.getInstance("HmacSHA384")
    mac.init(secret)
    val hashString: Array[Byte] = mac.doFinal(preHashString.getBytes)
    Hex.encodeHexString(hashString)
  }

  /**
   * Take the relevant request headers and calculate what the digest should be
   * @param sharedSecret passphrase to encrypt with
   * @return Option containing the hmac digest, or None if any headers were missing
   */
  def calculateHmac(contentType: String, sha384Checksum: String, method: String, uri: String, sharedSecret: String, time:ZonedDateTime):Option[String] = try {
    val httpDate = httpDateFormatter.format(time)

    val string_to_sign = s"$uri\n$httpDate\n$contentType\n$sha384Checksum\n$method"
    val hmac = generateHMAC(sharedSecret, string_to_sign)
    Some(hmac)
  } catch {
    case e:java.util.NoSuchElementException=>
      logger.debug(e.toString)
      None
  }

  /**
   * Returns information about the available crypto algorithms on this platform
   * @return
   */
  def getAlgos:Array[(String,String,String)] = {
    for {
      provider <- Security.getProviders
      key <- provider.stringPropertyNames.asScala
    } yield Tuple3(provider.getName, key, provider.getProperty(key))

  }
}
