import java.io.File
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{FileIO, GraphDSL, Keep, RunnableGraph}
import org.specs2.mutable.Specification
import streamcomponents.{ChecksumSink, MMappedFileSource}
import testhelpers.AkkaTestkitSpecs2Support

import scala.concurrent.Await
import scala.concurrent.duration._

class ChecksumSinkSpec extends Specification {
  "ChecksumSink" should {
    "calculate the checksum of an incoming message correctly" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val src = FileIO.fromPath(new File("random.dat").toPath)


      val result = Await.result(src.toMat(new ChecksumSink("md5"))(Keep.right).run(), 30.seconds)
      result must beSome("d41d8cd98f00b204e9800998ecf8427e")
    }

    "behave correctly when reading from MMappedFileSource" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = ActorMaterializer.create(system)

      val sinkFactory = new ChecksumSink("md5")

      val stream = RunnableGraph.fromGraph(GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(new MMappedFileSource(new File("random.dat"),2*1024))
        src ~> sink
        ClosedShape
      })

      val result = Await.result(stream.run(), 30.seconds)
      result must beSome("d41d8cd98f00b204e9800998ecf8427e")
    }
  }
}
