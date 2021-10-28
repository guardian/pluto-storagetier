package com.gu.multimedia.mxscopy

import akka.stream.scaladsl.{FileIO, GraphDSL, Keep, RunnableGraph, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import akka.util.ByteString
import com.gu.multimedia.mxscopy.streamcomponents.{ChecksumSink, MMappedFileSource}
import org.specs2.mutable.Specification
import testhelpers.AkkaTestkitSpecs2Support

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class ChecksumSinkSpec extends Specification {
  "ChecksumSink" should {
    "calculate the checksum of an incoming message correctly" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = Materializer.matFromSystem

      val src = FileIO.fromPath(new File("random.dat").toPath)

      val result = Await.result(src.toMat(new ChecksumSink("md5"))(Keep.right).run(), 30.seconds)
      result must beSome("1206b834dd2a96bca05617a86eb62619")
    }

    "behave correctly when reading from MMappedFileSource" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = Materializer.matFromSystem

      val sinkFactory = new ChecksumSink("md5")

      val stream = RunnableGraph.fromGraph(GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(new MMappedFileSource(new File("random.dat"),2*1024))
        src ~> sink
        ClosedShape
      })

      val result = Await.result(stream.run(), 30.seconds)
      result must beSome("1206b834dd2a96bca05617a86eb62619")
    }

    "yield a failed Try if the upstream fails" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = Materializer.matFromSystem

      val sinkFactory = new ChecksumSink("md5")

      val stream = RunnableGraph.fromGraph(GraphDSL.create(sinkFactory) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(Source.failed[ByteString](new RuntimeException("splat")))
        src ~> sink
        ClosedShape
      })

      val result = Try { Await.result(stream.run(), 30.seconds) }
      result must beFailedTry
    }
  }
}
