package com.gu.multimedia.mxscopy

import com.gu.multimedia.mxscopy.helpers.MatrixStoreHelper
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.File

class MatrixStoreHelperSpec extends Specification with Mockito {
  "MatrixStoreHelper.getFileExt" should {
    "extract the file extension from a string" in {
      val result = MatrixStoreHelper.getFileExt("filename.ext")
      result must beSome("ext")
    }

    "return None if there is no file extension" in {
      val result = MatrixStoreHelper.getFileExt("filename")
      result must beNone
    }

    "return None if there is a dot but no file extension" in {
      val result = MatrixStoreHelper.getFileExt("filename.")
      result must beNone
    }

    "filter out something that is too long for an extension" in {
      val result = MatrixStoreHelper.getFileExt("filename.somethingreallylong")
      result must beNone
    }
  }

  "MatrixStoreHelper.metadataFromFilesystem" should {
    "look up filesystem metadata and convert it to MxsMetadata" in {
      val result = MatrixStoreHelper.metadataFromFilesystem(new File("build.sbt"))
      println(result.toString)
      result must beSuccessfulTry
    }
  }

}
