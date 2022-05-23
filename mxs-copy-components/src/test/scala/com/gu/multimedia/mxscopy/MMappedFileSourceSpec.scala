package com.gu.multimedia.mxscopy

import akka.stream.scaladsl.{GraphDSL, RunnableGraph}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.gu.multimedia.mxscopy.streamcomponents.{ChecksumSink, MMappedFileSource}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import testhelpers.AkkaTestkitSpecs2Support

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._

class MMappedFileSourceSpec extends Specification with Mockito {
  "MMappedFileSource" should {
    "correctly read in a large file of data" in new AkkaTestkitSpecs2Support {
      if (Option(System.getProperty("LARGE_FILE_TEST")).isDefined) {
        implicit val mat: Materializer = ActorMaterializer.create(system)
        val file = new File("testfiles/large-test-file.mp4")
        val sinkFactory = new ChecksumSink()

        val graph = GraphDSL.create(sinkFactory) { implicit builder =>
          sink =>
            import akka.stream.scaladsl.GraphDSL.Implicits._

            val src = builder.add(new MMappedFileSource(file).async)
            src ~> sink
            ClosedShape
        }

        val result = Await.result(RunnableGraph.fromGraph(graph).run(), 30 seconds)
        //result.wasSuccessful must beTrue
        result must beSome("05d5bb67af3037acf01d1bc42615cc9a")
      }
    }
  }
}
