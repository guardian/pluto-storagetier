import akka.stream.Materializer
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.om.mxs.client.japi.{MxsObject, Vault}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global
import java.nio.file.{Path, Paths}
import java.util
import java.util.Map
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try

class FileCopierSpec extends Specification with Mockito {
  "FileCopier.maybeGetIndex" should {
    "return the index portion of a filename" in {
      implicit val mat:Materializer = mock[Materializer]
      val elem = ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file-2.ext")), None)

      val toTest = new FileCopier() {
        def callMaybeGetIndex(elem:ObjectMatrixEntry) = maybeGetIndex(elem)
      }

      toTest.callMaybeGetIndex(elem) must beSome(2)
    }

    "return the index portion of a filename without extension" in {
      implicit val mat:Materializer = mock[Materializer]
      val elem = ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file-2")), None)

      val toTest = new FileCopier() {
        def callMaybeGetIndex(elem:ObjectMatrixEntry) = maybeGetIndex(elem)
      }

      toTest.callMaybeGetIndex(elem) must beSome(2)
    }

    "return 0 if there is no index" in {
      implicit val mat:Materializer = mock[Materializer]
      val elem = ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file.ext")), None)

      val toTest = new FileCopier() {
        def callMaybeGetIndex(elem:ObjectMatrixEntry) = maybeGetIndex(elem)
      }

      toTest.callMaybeGetIndex(elem) must beSome(0)
    }

    "return None if the object does not have a filename set" in {
      implicit val mat:Materializer = mock[Materializer]
      val elem = ObjectMatrixEntry("file1",Some(MxsMetadata.empty), None)

      val toTest = new FileCopier() {
        def callMaybeGetIndex(elem:ObjectMatrixEntry) = maybeGetIndex(elem)
      }

      toTest.callMaybeGetIndex(elem) must beNone
    }
  }

  "FileCopier.updateFilenameIfRequired" should {
    "return the original filename if there are no matching files on the destination" in {
      implicit val mat:Materializer = mock[Materializer]
      val vault = mock[Vault]

      val toTest = new FileCopier() {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(Seq())
      }

      val result = Await.result(toTest.updateFilenameIfRequired(vault, "/path/to/some/file.ext"), 1.second)
      result mustEqual("/path/to/some/file.ext")
    }

    "return the next index in sequence if there are matching files" in {
      implicit val mat:Materializer = mock[Materializer]
      val vault = mock[Vault]
      val results = Seq(
        ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file.ext")), None),
        ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file-1.ext")), None),
        ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file-3.ext")), None),
      )
      val toTest = new FileCopier() {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(results)
      }

      val result = Await.result(toTest.updateFilenameIfRequired(vault, "/path/to/some/file.ext"), 1.second)
      result mustEqual("/path/to/some/file-4.ext")
    }

    "return the next index in sequence if there is only one matching file" in {
      implicit val mat:Materializer = mock[Materializer]
      val vault = mock[Vault]
      val results = Seq(
        ObjectMatrixEntry("file1",Some(MxsMetadata.empty.withValue("MXFS_PATH", "/path/to/some/file.ext")), None)
      )
      val toTest = new FileCopier() {
        override protected def callFindByFilenameNew(vault: Vault, fileName: String): Future[Seq[ObjectMatrixEntry]] = Future(results)
      }

      val result = Await.result(toTest.updateFilenameIfRequired(vault, "/path/to/some/file.ext"), 1.second)
      result mustEqual("/path/to/some/file-1.ext")
    }
  }

  "FileCopier.copyFileToMatrixStore" should {
    "perform an upload without an existing object id and return new object id with checksum" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("some-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, None), 3.seconds)

      result must beEqualTo(Right("some-object-id"))
    }

    "perform an upload with an existing objectId with different file size and return objectId" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getContextMap() = {
          new util.HashMap[String, String]()
        }

        override protected def setContextMap(contextMap: Map[String, String]) = {
        }

        override protected def getOMFileMd5(mxsFile: MxsObject) = {
          Future(Try("md5-checksum-1"))
        }

        override protected def getChecksumFromPath(filePath: Path): Future[Option[String]] = {
          Future(Some("md5-checksum-1"))
        }

        override protected def getSizeFromPath(filePath: Path) = {
          1000L
        }

        override protected def getSizeFromMxs(mxsFile: MxsObject) = {
          10L
        }
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      result must beEqualTo(Right("existing-object-id"))
    }

    "perform an upload with an existing objectId with different checksum and return objectId" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getContextMap() = {
          new util.HashMap[String, String]()
        }

        override protected def setContextMap(contextMap: Map[String, String]) = {
        }

        override protected def getOMFileMd5(mxsFile: MxsObject) = {
          Future(Try("md5-checksum-1"))
        }

        override protected def getChecksumFromPath(filePath: Path): Future[Option[String]] = {
          Future(Some("md5-checksum-2"))
        }

        override protected def getSizeFromPath(filePath: Path) = {
          10L
        }

        override protected def getSizeFromMxs(mxsFile: MxsObject) = {
          10L
        }
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      result must beEqualTo(Right("existing-object-id"))
    }

    "return Right with objectId if an object with same size and checksum already exist in ObjectMatrix" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getContextMap() = {
          new util.HashMap[String, String]()
        }

        override protected def setContextMap(contextMap: Map[String, String]) = {
        }

        override protected def getOMFileMd5(mxsFile: MxsObject) = {
          Future(Try("md5-checksum-1"))
        }

        override protected def getChecksumFromPath(filePath: Path): Future[Option[String]] = {
          Future(Some("md5-checksum-1"))
        }

        override protected def getSizeFromPath(filePath: Path) = {
          10L
        }

        override protected def getSizeFromMxs(mxsFile: MxsObject) = {
          10L
        }
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      result must beEqualTo(Right("existing-object-id"))
    }

    "return Right with objectId if an object with same id doesn't exist in ObjectMatrix" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      //workaround from https://stackoverflow.com/questions/3762047/throw-checked-exceptions-from-mocks-with-mockito
      mockVault.getObject(any) answers( (x:Any)=> throw new IOException("Invalid object, it does not exist (error 306)"))
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      result must beEqualTo(Right("existing-object-id"))
    }

    "return Left with error message when unknown Exception is thrown by ObjectMatrix" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      //workaround from https://stackoverflow.com/questions/3762047/throw-checked-exceptions-from-mocks-with-mockito
      mockVault.getObject(any) answers( (x:Any)=> throw new RuntimeException("Some unknown exception"))
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      result must beEqualTo(Left("ObjectMatrix error: Some unknown exception"))
    }
  }
}
