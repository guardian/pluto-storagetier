import akka.stream.Materializer
import com.gu.multimedia.mxscopy.models.{MxsMetadata, ObjectMatrixEntry}
import com.om.mxs.client.japi.{MxsObject, Vault}
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global
import java.nio.file.{Path, Paths}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

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

      val mockGetSize = mock[(Path)=>Long]
      mockGetSize.apply(any) returns 1234L

      val mockFindMatchingFiles = mock[(Vault, Path, Long)=>Future[Seq[ObjectMatrixEntry]]]
      mockFindMatchingFiles(any,any,any) returns Future(Seq())

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getSizeFromPath(filePath: Path): Long = mockGetSize(filePath)

        override def findMatchingFilesOnNearline(vault: Vault, filePath: Path, fileSize: Long): Future[Seq[ObjectMatrixEntry]] = mockFindMatchingFiles(vault, filePath, fileSize)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, None), 3.seconds)

      there was one(mockGetSize).apply(mockFilePath)
      there was one(mockFindMatchingFiles).apply(mockVault, mockFilePath, 1234L)
      there was one(mockCopyUsingHelper).apply(mockVault, "file-name.mp4", mockFilePath)
      result must beRight("some-object-id")
    }

    "perform an upload with an existing objectId with different file size and return objectId" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))
      val mockGetSize = mock[(Path)=>Long]
      mockGetSize.apply(any) returns 1234L

      val mockFindMatchingFiles = mock[(Vault, Path, Long)=>Future[Seq[ObjectMatrixEntry]]]
      mockFindMatchingFiles(any,any,any) returns Future(Seq())

      val mockVerifyChecksum = mock[(Path, Seq[MxsObject], Option[String])=>Future[Option[String]]]
      mockVerifyChecksum.apply(any,any,any) returns Future(None)

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getContextMap() = {
          Some(new java.util.HashMap[String, String]())
        }

        override protected def setContextMap(contextMap: java.util.Map[String, String]) = {
        }

        override protected def getOMFileMd5(mxsFile: MxsObject) = {
          Future(Try("md5-checksum-1"))
        }

        override protected def getChecksumFromPath(filePath: Path): Future[Option[String]] = {
          Future(Some("md5-checksum-1"))
        }

        override protected def getSizeFromPath(filePath: Path): Long = mockGetSize(filePath)

        override def findMatchingFilesOnNearline(vault: Vault, filePath: Path, fileSize: Long): Future[Seq[ObjectMatrixEntry]] = mockFindMatchingFiles(vault, filePath, fileSize)

        override protected def verifyChecksumMatch(filePath: Path, potentialFiles: Seq[MxsObject], maybeLocalChecksum: Option[String]): Future[Option[String]] = mockVerifyChecksum(filePath, potentialFiles, maybeLocalChecksum)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      there was one(mockGetSize).apply(mockFilePath)
      there was one(mockFindMatchingFiles).apply(mockVault, mockFilePath, 1234L)
      there was one(mockCopyUsingHelper).apply(mockVault, "file-name.mp4", mockFilePath)
      there was one(mockVerifyChecksum).apply(mockFilePath,Seq(),None) //the file sizes do not match so we should not have undertaken the checksum verify
      result must beRight("existing-object-id")
    }

    "perform an upload with an existing objectId with different checksum and return objectId" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))
      val mockGetSize = mock[(Path)=>Long]
      mockGetSize.apply(any) returns 1234L

      val mockFindMatchingFiles = mock[(Vault, Path, Long)=>Future[Seq[ObjectMatrixEntry]]]
      mockFindMatchingFiles(any,any,any) returns Future(Seq(
        ObjectMatrixEntry("existing-object-id",Some(MxsMetadata(
          Map("__mxs__length"->"1234"),
          Map(),
          Map(),
          Map()
        )), None)
      ))

      val mockExistingObject = mock[MxsObject]

      val mockOpenMxsObject = mock[(Vault, String)=>Try[MxsObject]]
      mockOpenMxsObject.apply(any, any) returns Success(mockExistingObject)

      val mockVerifyChecksum = mock[(Path, Seq[MxsObject], Option[String])=>Future[Option[String]]]
      mockVerifyChecksum.apply(any,any,any) returns Future(None)

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getContextMap() = {
          Some(new java.util.HashMap[String, String]())
        }

        override protected def setContextMap(contextMap: java.util.Map[String, String]) = {
        }

        override def openMxsObject(vault: Vault, oid: String): Try[MxsObject] = mockOpenMxsObject(vault, oid)

        override protected def getSizeFromPath(filePath: Path): Long = mockGetSize(filePath)

        override def findMatchingFilesOnNearline(vault: Vault, filePath: Path, fileSize: Long): Future[Seq[ObjectMatrixEntry]] = mockFindMatchingFiles(vault, filePath, fileSize)

        override protected def verifyChecksumMatch(filePath: Path, potentialFiles: Seq[MxsObject], maybeLocalChecksum: Option[String]): Future[Option[String]] = mockVerifyChecksum(filePath, potentialFiles, maybeLocalChecksum)

      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      there was one(mockFindMatchingFiles).apply(mockVault, mockFilePath, 1234L)
      there was one(mockOpenMxsObject).apply(mockVault, "existing-object-id")
      there was one(mockVerifyChecksum).apply(mockFilePath, Seq(mockExistingObject), None)

      result must beRight("existing-object-id")
    }

    "return Right with objectId if an object with same size and checksum already exist in ObjectMatrix" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockObject = mock[MxsObject]
      mockVault.getObject(any) returns mockObject
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val mockGetSize = mock[(Path)=>Long]
      mockGetSize.apply(any) returns 1234L

      val mockFindMatchingFiles = mock[(Vault, Path, Long)=>Future[Seq[ObjectMatrixEntry]]]
      mockFindMatchingFiles(any,any,any) returns Future(Seq(
        ObjectMatrixEntry("existing-object-id",Some(MxsMetadata(
          Map("__mxs__length"->"1234"),
          Map(),
          Map(),
          Map()
        )), None)
      ))

      val mockExistingObject = mock[MxsObject]

      val mockOpenMxsObject = mock[(Vault, String)=>Try[MxsObject]]
      mockOpenMxsObject.apply(any, any) returns Success(mockExistingObject)

      val mockVerifyChecksum = mock[(Path, Seq[MxsObject], Option[String])=>Future[Option[String]]]
      mockVerifyChecksum.apply(any,any,any) returns Future(Some("existing-object-id"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override protected def getContextMap() = {
          Some(new java.util.HashMap[String, String]())
        }

        override protected def setContextMap(contextMap: java.util.Map[String, String]) = {
        }

        override protected def getOMFileMd5(mxsFile: MxsObject) = {
          Future(Try("md5-checksum-1"))
        }

        override protected def getChecksumFromPath(filePath: Path): Future[Option[String]] = {
          Future(Some("md5-checksum-1"))
        }

        override def openMxsObject(vault: Vault, oid: String): Try[MxsObject] = mockOpenMxsObject(vault, oid)

        override protected def getSizeFromPath(filePath: Path): Long = mockGetSize(filePath)

        override def findMatchingFilesOnNearline(vault: Vault, filePath: Path, fileSize: Long): Future[Seq[ObjectMatrixEntry]] = mockFindMatchingFiles(vault, filePath, fileSize)

        override protected def verifyChecksumMatch(filePath: Path, potentialFiles: Seq[MxsObject], maybeLocalChecksum: Option[String]): Future[Option[String]] = mockVerifyChecksum(filePath, potentialFiles, maybeLocalChecksum)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      there was one(mockGetSize).apply(mockFilePath)
      there was one(mockFindMatchingFiles).apply(mockVault, mockFilePath, 1234L)
      there was one(mockOpenMxsObject).apply(mockVault, "existing-object-id")
      there was one(mockVerifyChecksum).apply(mockFilePath, Seq(mockExistingObject), None)
      there was no(mockCopyUsingHelper).apply(any,any,any)
      result must beRight("existing-object-id")
    }

    //this test was removed as it is now functionally identical to the first

    "return Left with error message when unknown Exception is thrown by ObjectMatrix" in {
      implicit val mat:Materializer = mock[Materializer]

      val mockVault = mock[Vault]
      val mockFilePath = Paths.get("/some/path/", "file-name.mp4")

      val mockCopyUsingHelper = mock[(Vault, String, Path)=> Future[Right[Nothing, String]]]
      mockCopyUsingHelper.apply(any, any, any) returns Future(Right("existing-object-id"))

      val mockGetSize = mock[Path=>Long]
      mockGetSize.apply(any) returns 1234

      val mockFindMatchingFiles = mock[(Vault, Path, Long)=>Future[Seq[ObjectMatrixEntry]]]
      mockFindMatchingFiles.apply(any,any,any) returns Future.failed(new RuntimeException("Some unknown exception"))

      val toTest = new FileCopier() {
        override protected def copyUsingHelper(vault: Vault, fileName: String, filePath: Path): Future[Right[Nothing, String]] =
          mockCopyUsingHelper(vault, fileName, filePath)

        override def getSizeFromPath(filePath: Path): Long = mockGetSize(filePath)

        override def findMatchingFilesOnNearline(vault: Vault, filePath: Path, fileSize: Long): Future[Seq[ObjectMatrixEntry]] = mockFindMatchingFiles(vault, filePath, fileSize)
      }

      val result = Await.result(toTest.copyFileToMatrixStore(mockVault, "file-name.mp4", mockFilePath, Some("existing-object-id")),
        3.seconds)

      there was one(mockGetSize).apply(mockFilePath)
      result must beLeft("ObjectMatrix error: Some unknown exception")
    }
  }

}
