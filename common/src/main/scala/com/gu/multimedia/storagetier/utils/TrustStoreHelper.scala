package utils
import java.io.FileInputStream
import java.security.KeyStore

import javax.net.ssl.{SSLContext, TrustManager, TrustManagerFactory, X509TrustManager}
import java.security.KeyStore
import java.security.cert.{CertificateException, X509Certificate}
import sun.security.ssl.SSLContextImpl
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * helper that builds a custom trust manager implementation that supports multiple keystores
 * (i.e. the default one AND a custom one)
 */
object TrustStoreHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * obtain an X509TrustManager object based on the Java Keystore trust store at the given file path
   * @param keyStorePath path of a .jks file to open
   * @return a Try with either the X509TrustManager or an error
   */
  def getCustomTrustManager(keyStorePath:String, passwordString:Option[String]):Try[X509TrustManager] = {
    try {
      val myKeys = new FileInputStream(keyStorePath)
      try {
        // Do the same with your trust store this time
        // Adapt how you load the keystore to your needs
        val myTrustStore = KeyStore.getInstance(KeyStore.getDefaultType)
        myTrustStore.load(myKeys, passwordString.map(_.toCharArray).orNull)

        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(myTrustStore)
        val trustManagerList = tmf.getTrustManagers.filter(_.isInstanceOf[X509TrustManager])

        if (trustManagerList.isEmpty) {
          Failure(new RuntimeException(s"Could not find x509 trust manager in the provided store $keyStorePath"))
        }

        Success(trustManagerList.head.asInstanceOf[X509TrustManager])
      } catch {
        case ex: Throwable => Failure(ex)
      } finally {
        myKeys.close()
      }
    } catch {
      case ex: Throwable=>Failure(ex)
    }
  }

  /**
   * obtain an X509TrustManager object representing the default key store
   * @return a Try with either the X509TrustManager or an error
   */
  def getDefaultTrustManager:Try[X509TrustManager] = Try {
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    // Using null here initialises the TMF with the default trust store.
    tmf.init(null.asInstanceOf[KeyStore])

    val trustManagerList = tmf.getTrustManagers.filter(_.isInstanceOf[X509TrustManager])

    if(trustManagerList.isEmpty){
      throw new RuntimeException("Could not find default trust manager")
    }

    trustManagerList.head.asInstanceOf[X509TrustManager]
  }

  /**
   * build a custom X509TrustManager that will delegate to one of a list of trust managers
   * @param trustManagersList a sequence of subsequent trust managers to try. It's assumed that the "default" one is the first in the list.
   * @return an X509TrustManager object
   */
  def customX509TrustManager(trustManagersList:Seq[X509TrustManager]) = new X509TrustManager {
    /**
     * checkClientTrusted delegates to defaultTrustManager only
     * @param x509Certificates
     * @param authType
     * @throws
     */
    @throws[CertificateException]
    override def checkClientTrusted(x509Certificates: Array[X509Certificate], authType: String): Unit = trustManagersList.head.checkClientTrusted(x509Certificates, authType)

    protected def recursiveCheckAdditional(x509Certificates: Array[X509Certificate], authType: String, toCheck:X509TrustManager, tail:Seq[X509TrustManager], count:Integer):Try[Unit] = {
      Try {
        toCheck.checkServerTrusted(x509Certificates, authType)
      } match {
        case itworked @ Success(_)=>itworked
        case failure @ Failure(err)=>
          if(err.isInstanceOf[CertificateException]) {
            logger.debug(s"Cert auth failed at $count:", err)
            if (tail.isEmpty)
              Failure(err)
            else
              recursiveCheckAdditional(x509Certificates, authType, tail.head, tail.tail, count + 1)
          } else {
            failure
          }
      }
    }

    /**
     * recurse through all given X509TrustManagers until we find one that passes. If none pass then fail.
     * this is a Java method so expects an exception to be thrown if it fails
     * @param x509Certificates certs to verify
     * @param authType authentication type
     */
    @throws[CertificateException]
    override def checkServerTrusted(x509Certificates: Array[X509Certificate], authType: String): Unit = recursiveCheckAdditional(x509Certificates,authType, trustManagersList.head, trustManagersList.tail,0) match {
      case Success(_)=>()
      case Failure(err)=>throw err
    }

    override def getAcceptedIssuers: Array[X509Certificate] = trustManagersList.flatMap(_.getAcceptedIssuers).toArray
  }

  /**
   * convenience function that builds a custom TrustManager, configures an SSLContext to use it and then sets this context
   * as the default one
   * @param keyStorePaths sequence of paths to supplementary keystores
   * @return
   */
  def setupTS(keyStorePaths:Seq[String]) = {
    val trustManagerOrErrorList = Seq(getDefaultTrustManager) ++ keyStorePaths.map(path=>getCustomTrustManager(path,None))

    val failures = trustManagerOrErrorList.collect({case Failure(err)=>err})
    if(failures.nonEmpty){
      failures.foreach(err=>
        logger.error("Could not initialise trust stores: ", err)
      )
      Failure(failures.head)
    } else {
      val trustManagerList = trustManagerOrErrorList.collect({case Success(tm)=>tm})
      val customTm = customX509TrustManager(trustManagerList)

      val sslContext = SSLContext.getInstance("TLS")
      sslContext.init(null, Array(customTm), null)

      // You don't have to set this as the default context,
      // it depends on the library you're using.
      SSLContext.setDefault(sslContext)
      Success(sslContext)
    }
  }
}
