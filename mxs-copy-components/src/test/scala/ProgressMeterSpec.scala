import akka.stream.{ActorMaterializer, Attributes, Materializer}
import akka.stream.scaladsl.{Flow, Keep, Source}
import models.CopyReport
import org.specs2.mutable.Specification
import streamcomponents.{ListCopyFile, ProgressMeterAndReport}
import testhelpers.AkkaTestkitSpecs2Support

import scala.concurrent.Await
import scala.concurrent.duration._

class ProgressMeterSpec extends Specification {
  "ProgressMeterAndReport" should {
    "receive CopyReport data and log it" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)
      val fakeData = Seq(
        CopyReport("file1","1234",Some("checksum"),1*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),2*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),3*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),4*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),5*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),6*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),7*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),8*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),9*1048576, false, None),
        CopyReport("file1","1234",Some("checksum"),10*1048576, false, None),

      )
      //it seems that .delay() does not work as per the docs, it does initialDelay() instead...
      //see https://github.com/akka/akka/issues/24641 for the reasons behind the buffer attributes
      val src = Source.fromIterator(()=>fakeData.toIterator).delay(1000 millis).withAttributes(Attributes(Attributes.InputBuffer(15, 15)))
      val stream = src.toMat(new ProgressMeterAndReport(Some(10),Some(55*1048576)))(Keep.right)
      val result = Await.result(stream.run(), 30 seconds)

      println(result)
      result.copyCount mustEqual 10
      result.elapsedTimeInMillis must beGreaterThan(1000L)
      result.gbCopied mustEqual 0.0537109375

    }
  }
}
