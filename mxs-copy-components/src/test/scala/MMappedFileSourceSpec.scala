import java.io.File
import java.nio.file.Path
import akka.Done
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.stream.scaladsl.{FileIO, GraphDSL, RunnableGraph, Sink, Source}
import org.specs2.mutable.Specification
import org.specs2.mock.Mockito
import streamcomponents.{ChecksumSink, MMappedFileSource}
import testhelpers.AkkaTestkitSpecs2Support

import scala.concurrent.Await
import scala.concurrent.duration._

class MMappedFileSourceSpec extends Specification with Mockito {
  "MMappedFileSource" should {
    "correctly read in a large file of data" in new AkkaTestkitSpecs2Support {
      if (Option(System.getProperty("LARGE_FILE_TEST")).isDefined) {
        implicit val mat: Materializer = ActorMaterializer.create(system)
        val file = new File("testfiles/large-test-file.mp4")
        val sinkFactory = new ChecksumSink()
        //val sinkFactory = FileIO.toPath(new File("testfiles/testout.mp4").toPath)

        val graph = GraphDSL.create(sinkFactory) { implicit builder =>
          sink =>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val src = builder.add(new MMappedFileSource(file).async)
            //val src = builder.add(Source.fromIterator(()=>Seq(1,2,3,4).toIterator))
            src ~> sink
            ClosedShape
        }

        val result = Await.result(RunnableGraph.fromGraph(graph).run(), 30 seconds)
        //result.wasSuccessful must beTrue
        result must beSome("adff7d2ede6489c424568db68f90ef27")
      }
    }
  }
}
