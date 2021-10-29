package com.gu.multimedia.mxscopy.streamcomponents

import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorMaterializer, ClosedShape, Materializer}
import com.om.mxs.client.japi.{MxsInputStream, MxsObject, UserInfo, Vault}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import testhelpers.AkkaTestkitSpecs2Support

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MatrixStoreFileSourceSpec extends Specification with Mockito {
  "MatrixStoreFileSource" should {
    "fail the stream with an error if getObject throws an exception" in new AkkaTestkitSpecs2Support {
      implicit val mat:Materializer = Materializer.matFromSystem

      val mockedVault = mock[Vault]
      mockedVault.getObject(any) throws new RuntimeException("test fault")
      mockedVault.getId returns "test-vault-id"
      val toTest = new MatrixStoreFileSource(mockedVault, "some-source-id")
      val sinkFac = Sink.ignore
      val graph = GraphDSL.createGraph(sinkFac) { implicit builder=> sink=>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val src = builder.add(toTest)
        src ~> sink
        ClosedShape
      }

      val result = Try { Await.result(RunnableGraph.fromGraph(graph).run(), 2.seconds)}

      result must beFailedTry
      result.failed.get.getMessage mustEqual "test fault"
    }
  }

  "MatrixStoreFileSource.tryToGetStream" should {
    "not try to get a stream if the file does not exist" in new AkkaTestkitSpecs2Support {
      val mockedVault = mock[Vault]
      val mockedObject = mock[MxsObject]
      mockedVault.getObject(any) returns mockedObject
      mockedObject.exists() returns false
      mockedObject.newInputStream() returns mock[MxsInputStream]
      mockedObject.getVault returns mockedVault

      val toTest = new MatrixStoreFileSource(mockedVault, "some-source-id") {
        def callTryToGetStream(vault:Vault)(implicit ec:ExecutionContext) = tryToGetStream(vault)(ec)
      }

      val result = Try { Await.result(toTest.callTryToGetStream(mockedVault), 10.seconds) }

      there was one(mockedVault).getObject("some-source-id")
      there was one(mockedObject).exists()
      there was no(mockedObject).newInputStream()

      result must beFailedTry
      result.failed.get.getMessage contains("does not exist") must beTrue
    }
  }
}
