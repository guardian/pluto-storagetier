package com.gu.multimedia.storagetier.vidispine

import org.specs2.mutable.Specification
import io.circe.generic.auto._
import io.circe.syntax._

class FileDocumentSpec extends Specification{
  "FileDocument" should {
    "parse even if the 'hash' field is missing" in {
      val sample_data = """{
                      |  "id": "VX-806879",
                      |  "path": "VX-806879.wav",
                      |  "uri": [
                      |    "file:///srv/Path/To/Deliverables/VX-806879.wav"
                      |  ],
                      |  "state": "CLOSED",
                      |  "size": -1,
                      |  "timestamp": "2021-11-23T18:58:09.043+0000",
                      |  "refreshFlag": 1,
                      |  "storage": "VX-8",
                      |  "metadata": {}
                      |}""".stripMargin

      val result = io.circe.parser.parse(sample_data).flatMap(_.as[FileDocument])

      result must beRight
      result.map(_.state) must beRight("CLOSED")
      result.map(_.id) must beRight("VX-806879")
      result.map(_.path) must beRight("VX-806879.wav")
      result.map(_.hash).getOrElse(Some("invalid")) must beNone
    }

    "parse with a valid 'hash' field" in {
      val sample_data = """{
                          |  "id": "VX-806879",
                          |  "path": "VX-806879.wav",
                          |  "uri": [
                          |    "file:///srv/Path/To/Deliverables/VX-806879.wav"
                          |  ],
                          |  "state": "CLOSED",
                          |  "hash": "c46b0b6a39f23559faafd9c3550def83",
                          |  "size": 12345,
                          |  "timestamp": "2021-11-23T18:58:09.043+0000",
                          |  "refreshFlag": 1,
                          |  "storage": "VX-8",
                          |  "metadata": {}
                          |}""".stripMargin

      val result = io.circe.parser.parse(sample_data).flatMap(_.as[FileDocument])

      result must beRight
      result.map(_.state) must beRight("CLOSED")
      result.map(_.id) must beRight("VX-806879")
      result.map(_.path) must beRight("VX-806879.wav")
      result.map(_.hash).getOrElse(None) must beSome("c46b0b6a39f23559faafd9c3550def83")
    }
  }
}
