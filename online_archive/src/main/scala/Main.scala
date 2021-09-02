import com.gu.multimedia.storagetier.framework.DatabaseProvider

object Main {
  //this will raise an exception if it fails, so do it as the app loads so we know straight away.
  //for this reason, don't declare this as `lazy`; if it's gonna crash, get it over with.
  private val db = DatabaseProvider.get()

  def main(args:Array[String]) = {

  }
}
