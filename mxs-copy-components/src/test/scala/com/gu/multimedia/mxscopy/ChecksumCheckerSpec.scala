package com.gu.multimedia.mxscopy

import akka.stream.Materializer
import com.om.mxs.client.japi.MxsObject
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

class ChecksumCheckerSpec extends Specification with Mockito {

  "ChecksumChecker.verifyChecksumMatch" should {
    "determine the local file checksum once and verify it against the checksums of all the remote files" in {
      implicit val mat:Materializer = mock[Materializer]

      val fakeFiles = Seq(mock[MxsObject], mock[MxsObject], mock[MxsObject], mock[MxsObject])
      val mockGetChecksumFromPath = mock[Path=>Future[Option[String]]]
      mockGetChecksumFromPath.apply(any) returns Future(Some("the-correct-checksum"))

      val mockGetOMFileMD5 = mock[MxsObject=>Future[Try[String]]]
      mockGetOMFileMD5.apply(fakeFiles.head) returns Future(Success("wrong-checksum-1"))
      mockGetOMFileMD5.apply(fakeFiles(1)) returns Future(Success("wrong-checksum-2"))
      mockGetOMFileMD5.apply(fakeFiles(2)) returns Future(Success("the-correct-checksum"))
      mockGetOMFileMD5.apply(fakeFiles(3)) returns Future(Success("wrong-checksum-4"))

      fakeFiles(2).getId returns "the-correct-file"

      val toTest = new ChecksumChecker() {
        override def getChecksumFromPath(filePath: Path): Future[Option[String]] = mockGetChecksumFromPath(filePath)
        override def getOMFileMd5(mxsFile: MxsObject): Future[Try[String]] = mockGetOMFileMD5(mxsFile)
      }

      val result = Await.result(toTest.verifyChecksumMatch(Paths.get("/path/to/test.file"), fakeFiles), 2.seconds)

      result must beSome("the-correct-file")
      there was one(mockGetChecksumFromPath).apply(Paths.get("/path/to/test.file"))
      there were three(mockGetOMFileMD5).apply(any)
      there was one(mockGetOMFileMD5).apply(fakeFiles.head)
      there was one(mockGetOMFileMD5).apply(fakeFiles(1))
      there was one(mockGetOMFileMD5).apply(fakeFiles(2))
      there was no(mockGetOMFileMD5).apply(fakeFiles(3))
    }

    "check everything and return no match" in {
      implicit val mat:Materializer = mock[Materializer]

      val fakeFiles = Seq(mock[MxsObject], mock[MxsObject], mock[MxsObject], mock[MxsObject])
      val mockGetChecksumFromPath = mock[Path=>Future[Option[String]]]
      mockGetChecksumFromPath.apply(any) returns Future(Some("the-correct-checksum"))

      val mockGetOMFileMD5 = mock[MxsObject=>Future[Try[String]]]
      mockGetOMFileMD5.apply(fakeFiles.head) returns Future(Success("wrong-checksum-1"))
      mockGetOMFileMD5.apply(fakeFiles(1)) returns Future(Success("wrong-checksum-2"))
      mockGetOMFileMD5.apply(fakeFiles(2)) returns Future(Success("wrong-checksum-3"))
      mockGetOMFileMD5.apply(fakeFiles(3)) returns Future(Success("wrong-checksum-4"))

      fakeFiles(2).getId returns "the-correct-file"

      val toTest = new ChecksumChecker() {
        override def getChecksumFromPath(filePath: Path): Future[Option[String]] = mockGetChecksumFromPath(filePath)
        override def getOMFileMd5(mxsFile: MxsObject): Future[Try[String]] = mockGetOMFileMD5(mxsFile)
      }

      val result = Await.result(toTest.verifyChecksumMatch(Paths.get("/path/to/test.file"), fakeFiles), 2.seconds)

      result must beNone
      there was one(mockGetChecksumFromPath).apply(Paths.get("/path/to/test.file"))
      there was one(mockGetOMFileMD5).apply(fakeFiles.head)
      there was one(mockGetOMFileMD5).apply(fakeFiles(1))
      there was one(mockGetOMFileMD5).apply(fakeFiles(2))
      there was one(mockGetOMFileMD5).apply(fakeFiles(3))
    }
  }


}
