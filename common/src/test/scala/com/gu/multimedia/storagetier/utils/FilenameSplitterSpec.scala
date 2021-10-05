package com.gu.multimedia.storagetier.utils

import org.specs2.mutable.Specification

class FilenameSplitterSpec extends Specification {
  "FilenameSplitter.apply" should {
    "split a filename into a base, extension tuple" in {
      FilenameSplitter("somefile.xtn") mustEqual ("somefile", Some(".xtn"))
    }
    "return None for the extension if there isn't one" in {
      FilenameSplitter("somefile") mustEqual ("somefile", None)
    }
    "only take the last dot as an extension" in {
      FilenameSplitter("somefile.with.dots.xtn") mustEqual ("somefile.with.dots", Some(".xtn"))
    }
  }

  "FilenameSplitter.insertPostFilename" should {
    "insert an extra part into the filename" in {
      FilenameSplitter.insertPostFilename("somefile.xtn","_prox") mustEqual "somefile_prox.xtn"
    }
  }
}
