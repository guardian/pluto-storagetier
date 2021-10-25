package com.gu.multimedia.storagetier.plutocore

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.Implicits.global

class AssetFolderLookupSpec extends Specification with Mockito{
  "AssetFolderLookup.relativizeFilePath" should {
    "correctly wrap a call to relativizeFilePath" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      val fakeConfig = PlutoCoreConfig("test","test",Paths.get("/path/to/assetfolders"))
      val toTest = new AssetFolderLookup(fakeConfig) {
        def callRelativize(path:Path) = relativizeFilePath(path)
      }

      toTest.callRelativize(Paths.get("/path/to/assetfolders/wg/comm/proj/footage/media.mp4")) must beRight(Paths.get("wg/comm/proj/footage/media.mp4"))
    }

    "return a Left if the target path is not within the base" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      val fakeConfig = PlutoCoreConfig("test","test",Paths.get("/path/to/assetfolders"))
      val toTest = new AssetFolderLookup(fakeConfig) {
        def callRelativize(path:Path) = relativizeFilePath(path)
      }

      toTest.callRelativize(Paths.get("/usr/local/bin/somethingforbidden")) must beLeft("/usr/local/bin/somethingforbidden is not below the asset folder root of /path/to/assetfolders")
    }

    "return a Left if the paths are not relative" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      val fakeConfig = PlutoCoreConfig("test","test",Paths.get("invalid/path/one"))
      val toTest = new AssetFolderLookup(fakeConfig) {
        def callRelativize(path:Path) = relativizeFilePath(path)
      }

      toTest.callRelativize(Paths.get("another/wrong/path")) must beLeft("'other' is different type of Path")
    }
  }

  "AssetFolderLookup.findExpectedAssetFolder" should {
    "chop out the first three path elements" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      val fakeConfig = PlutoCoreConfig("test","test",Paths.get("/path/to/assetfolders"))
      val toTest = new AssetFolderLookup(fakeConfig) {
        def callFindExpected(path:Path) = findExpectedAssetfolder(path)
      }

      toTest.callFindExpected(Paths.get("wg/comm/proj/footage/somefile.mp4")) must beRight(Paths.get("wg/comm/proj"))
    }

    "return Left if there are insufficient path elements" in {
      implicit val mat = mock[Materializer]
      implicit val system = mock[ActorSystem]
      val fakeConfig = PlutoCoreConfig("test","test",Paths.get("/path/to/assetfolders"))
      val toTest = new AssetFolderLookup(fakeConfig) {
        def callFindExpected(path:Path) = findExpectedAssetfolder(path)
      }

      toTest.callFindExpected(Paths.get("footage/somefile.mp4")) must beLeft
    }
  }

}
