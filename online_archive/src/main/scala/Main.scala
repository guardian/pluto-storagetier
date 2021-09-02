import com.gu.multimedia.storagetier.framework.DatabaseProvider

object Main {
  private implicit val db = DatabaseProvider.get()

  def main(args:Array[String]) = {

  }
}
