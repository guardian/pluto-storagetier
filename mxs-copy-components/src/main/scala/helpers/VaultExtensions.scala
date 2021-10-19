package helpers

import akka.actor.ActorSystem
import com.om.mxs.client.japi.{MatrixStore, MxsObject, UserInfo, Vault}

import java.util.concurrent.TimeoutException
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

/**
 * this class defines some extension methods to the Vault class in order to prevent timeouts
 */
object VaultExtensions {
  def openVaultWithTimeout(userInfo:UserInfo, timeout:FiniteDuration)(implicit s:ActorSystem):Future[Vault] = {
    implicit val ec:ExecutionContext = s.dispatcher

    Future firstCompletedOf Seq(
      Future { Some(MatrixStore.openVault(userInfo))},
      timeoutFuture[Vault](timeout)
    ) flatMap {
      case None=>
        Future.failed(new TimeoutException("openVault timed out"))
      case Some(vault)=>
        Future(vault)
    }
  }

  protected def timeoutFuture[T](timeout:FiniteDuration)(implicit s:ActorSystem) = {
    val timeoutPromise = Promise[Option[T]]
    implicit val ec:ExecutionContext = s.dispatcher

    val timeoutRunnable = new Runnable {
      def run():Unit = timeoutPromise.complete(Success(None))
    }
    s.scheduler.scheduleOnce(timeout, timeoutRunnable)
    timeoutPromise.future
  }

  implicit class ExtendedVault(val vault:Vault) extends AnyVal {
    def getObjectWithTimeout(id:String, timeout:FiniteDuration)(implicit s:ActorSystem) = {
      implicit val ec:ExecutionContext = s.dispatcher

      Future firstCompletedOf Seq(
        Future { Some(vault.getObject(id)) },
        timeoutFuture[MxsObject](timeout)
      ) flatMap {
        case None=>
          Future.failed(new TimeoutException("getObject timed out"))
        case Some(mxsObject)=>
          Future(mxsObject)
      }

    }
  }
}
