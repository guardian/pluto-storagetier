package com.gu.multimedia.mxscopy

import akka.actor.ActorSystem
import com.om.mxs.client.japi.cred.Credentials
import com.om.mxs.client.japi.{MatrixStore, MatrixStoreConnection, Vault}
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

/**
 * describes the interface of MXSConnectionBuilder, which safely creates and disposes MatrixStore connections
 */
trait MXSConnectionBuilder {
  def getConnection():Try[MatrixStore]

  def withVaultFuture[T](vaultId:String)(cb:Vault=>Future[Either[String, T]])(implicit ec:ExecutionContext):Future[Either[String,T]]
}

/**
 * Real implementation of MXSConnectionBuilder.  This will call out to the appliance to retrieve vault references
 * @param hosts appliance hostnames/IP addresses as an array of strings
 * @param clusterId cluster ID
 * @param accessKeyId access key ID for the service account to use to connect
 * @param accessKeySecret access key secret for the service account to use to connect
 */
class MXSConnectionBuilderImpl(hosts: Array[String], clusterId:String, accessKeyId:String, accessKeySecret:String, maxIdleSeconds:Int=300)(implicit actorSystem: ActorSystem) extends MXSConnectionBuilder {
  //private vars are synchronised to object instance on access - that's why they need to be private and final
  private final var cachedConnection:Option[MatrixStore] = None
  private final var connectionLastRetrieved:Instant = Instant.now()

  private implicit val ec:ExecutionContext = actorSystem.dispatcher
  private val logger = LoggerFactory.getLogger(getClass)

  def build() = Try {
    logger.debug(s"Building new MXS connection to $hosts")
    val credentials = Credentials.newAccessKeyCredentials(accessKeyId, accessKeySecret)

    val conn = MatrixStoreConnection.builder().withHosts(hosts).withClusterId(clusterId).build()
    MatrixStore.builder()
      .withConnection(conn)
      .withCredentials(credentials)
      .build()
  }

  def getConnection():Try[MatrixStore] = this.synchronized {
    cachedConnection match {
      case Some(connection)=>
        logger.debug("Using cached MXS datastore connection")
        connectionLastRetrieved = Instant.now()
        Success(connection)
      case None=>
        logger.debug("Building new MXS datastore connection")
        connectionLastRetrieved = Instant.now()
        build().map(mxs=>{
          cachedConnection = Some(mxs)
          mxs
        })
    }
  }

  actorSystem.getScheduler.scheduleAtFixedRate(30.seconds, 30.seconds)(new Runnable {
    override def run(): Unit = {
      this.synchronized {
        cachedConnection match {
          case None=>
            logger.debug("No current MXS connection")
          case Some(mxs)=>
            val idleTime = Instant.now().getEpochSecond - connectionLastRetrieved.getEpochSecond
            logger.debug(s"Idle time of cached connection is $idleTime seconds")
            if(idleTime>=maxIdleSeconds) {
              logger.info(s"Terminating MXS connection as it has been idle for $idleTime seconds")
              mxs.dispose()
              cachedConnection = None
            }
        }
      }
    }
  })

  /**
   * initiates a connection to the configuration indicated by the builder, opens the given vault then runs the callback.
   * The callback is expected to return a Future of some type T.
   * Whether it succeeds or fails, the vault connection is then disposed.
   * Use this in preference to the static method if you have no pre-existing connection
   * @param vaultId vault ID to open
   * @param cb callback function which takes the Vault instance as a parameter and returns a Future of any type
   * @param ec implicitly defined execution context
   * @tparam T the type returned by the callback's Future
   * @return either the result of the callback, or a failed Try indicating some error in establishing the connection
   */
  def withVaultFuture[T](vaultId:String)(cb: Vault => Future[Either[String, T]])(implicit ec:ExecutionContext) = {
    Future.fromTry(getConnection()).flatMap(mxs=>{
      MXSConnectionBuilderImpl
        .withVaultFuture(mxs, vaultId)(cb)
    })
  }

  /**
   * Exactly the same as [[withVaultFuture]] but takes in multiple vault IDs and opens a connection to each of them.
   * The sequence of Vault instances passed to the callback should be in the same order as the vault IDs passed to the
   * function.
   * @param vaultIds sequence of strings representing the vault IDs to open. A failure is returned if any of these fail.
   * @param cb callback function, which needs to take a sequence of Vault objects representing the open vaults and return a Future of some type
   * @param ec implicitly provided execution context
   * @tparam T data type returned in the Future of the callback
   * @return the result of the callback, or a failure if we were not able to establish the connection
   */
  def withVaultsFuture[T](vaultIds:Seq[String])(cb: Seq[Vault]=>Future[T])(implicit ec:ExecutionContext) = {
    Future.fromTry(getConnection()).flatMap(mxs=>{
      Future
        .sequence(vaultIds.map(vid=>Future.fromTry(Try{mxs.openVault(vid)})))
        .flatMap(cb)
    })
  }
}

object MXSConnectionBuilderImpl {
  private val logger = LoggerFactory.getLogger(getClass)

  def withVault[T](mxs: MatrixStore, vaultId: String)(cb: (Vault) => Try[Either[String, T]]) = {
    Try {
      mxs.openVault(vaultId)
    } match {
      case Success(vault) =>
        val result = cb(vault)
        vault.dispose()
        result
      case Failure(err) =>
        logger.error(s"Could not establish vault connection: ${err.getMessage}", err)
        Success(Left(err.toString))
    }
  }

  def withVaultFuture[T](mxs:MatrixStore, vaultId: String)(cb: (Vault) => Future[Either[String, T]])(implicit ec:ExecutionContext) = {
    Try {
      mxs.openVault(vaultId)
    } match {
      case Success(vault) =>
        cb(vault).andThen(_=>vault.dispose())
      case Failure(err) =>
        logger.error(s"Could not establish vault connection: ${err.getMessage}", err)
        Future(Left(err.toString))
    }
  }
}

