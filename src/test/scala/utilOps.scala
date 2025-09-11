import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import org.apache.hadoop.hdfs.{MiniDFSCluster, DistributedFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataOutputStream, PathFilter}
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.util.Progressable
import dfs.{mkdir, mv, touch, exists, rm, cp, ls, cat}
import java.io.{FileNotFoundException, IOException}

// Trait to create mini hadoop cluster Any test can extend from it and use the mini cluster
trait MiniHDFSRunnerCoreOps extends TestSuite with BeforeAndAfterAll {
  protected var clusterTest: MiniDFSCluster = _

  // Spin up a mock Hadoop cluster before every tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    clusterTest = spinUpMiniCluster()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    clusterTest.shutdown()
  }

  /** function defined to set configuration of the cluster on build it
    * @return
    *   a miniDFSCluster
    */
  private def spinUpMiniCluster(): MiniDFSCluster = {
    val config = new Configuration()
    val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
    return cluster.build()
  }
}

@DoNotDiscover
class TestCp
    extends AnyFlatSpec
    with MiniHDFSRunnerCoreOps
    with should.Matchers {

  "cp" should "copy a file from source to destination" in {
    implicit val fs = clusterTest.getFileSystem()
    val srcFile = "test_source.txt"
    val dstFile = "test_destination.txt"
    
    // Create source file with content
    touch(srcFile)
    val outputStream = fs.create(new Path(srcFile))
    outputStream.write("test content".getBytes)
    outputStream.close()
    
    // Copy file
    cp(fs, srcFile, dstFile)
    
    // Verify file was copied
    assert(exists(dstFile))
    assert(exists(srcFile))
    
    // Verify content is the same
    val srcContent = cat(fs, srcFile)
    val dstContent = cat(fs, dstFile)
    assert(srcContent == dstContent)
    
    // Cleanup
    rm(srcFile)
    rm(dstFile)
  }

  it should "throw FileNotFoundException when source file does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val srcFile = "nonexistent_source.txt"
    val dstFile = "test_destination.txt"
    
    assertThrows[FileNotFoundException] {
      cp(fs, srcFile, dstFile)
    }
  }

  it should "throw IOException when source is a directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val srcDir = "test_source_dir"
    val dstFile = "test_destination.txt"
    
    // Create source directory
    mkdir(srcDir)
    
    assertThrows[IOException] {
      cp(fs, srcDir, dstFile)
    }
    
    // Cleanup
    rm.r(srcDir)
  }
}

@DoNotDiscover
class TestCpRecursive
    extends AnyFlatSpec
    with MiniHDFSRunnerCoreOps
    with should.Matchers {

  "cp.recursive" should "copy a directory and all its contents" in {
    implicit val fs = clusterTest.getFileSystem()
    val srcDir = "test_source_dir"
    val dstDir = "test_destination_dir"
    
    // Create source directory structure
    mkdir(s"$srcDir/subdir")
    touch(s"$srcDir/file1.txt")
    touch(s"$srcDir/subdir/file2.txt")
    
    // Add content to files
    val outputStream1 = fs.create(new Path(s"$srcDir/file1.txt"))
    outputStream1.write("content1".getBytes)
    outputStream1.close()
    
    val outputStream2 = fs.create(new Path(s"$srcDir/subdir/file2.txt"))
    outputStream2.write("content2".getBytes)
    outputStream2.close()
    
    // Copy directory recursively
    cp.recursive(fs, srcDir, dstDir)
    
    // Verify directory structure was copied
    assert(exists(dstDir))
    assert(exists(s"$dstDir/file1.txt"))
    assert(exists(s"$dstDir/subdir/file2.txt"))
    
    // Verify content is the same
    val srcContent1 = cat(fs, s"$srcDir/file1.txt")
    val dstContent1 = cat(fs, s"$dstDir/file1.txt")
    assert(srcContent1 == dstContent1)
    
    val srcContent2 = cat(fs, s"$srcDir/subdir/file2.txt")
    val dstContent2 = cat(fs, s"$dstDir/subdir/file2.txt")
    assert(srcContent2 == dstContent2)
    
    // Cleanup
    rm.r(srcDir)
    rm.r(dstDir)
  }

  it should "throw FileNotFoundException when source directory does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val srcDir = "nonexistent_source_dir"
    val dstDir = "test_destination_dir"
    
    assertThrows[FileNotFoundException] {
      cp.recursive(fs, srcDir, dstDir)
    }
  }

  it should "throw IOException when source is not a directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val srcFile = "test_source_file.txt"
    val dstDir = "test_destination_dir"
    
    // Create source file
    touch(srcFile)
    
    assertThrows[IOException] {
      cp.recursive(fs, srcFile, dstDir)
    }
    
    // Cleanup
    rm(srcFile)
  }
}

@DoNotDiscover
class TestLs
    extends AnyFlatSpec
    with MiniHDFSRunnerCoreOps
    with should.Matchers {

  "ls" should "list directory contents" in {
    implicit val fs = clusterTest.getFileSystem()
    val testDir = "test_ls_dir"
    
    // Create directory with files
    mkdir(testDir)
    touch(s"$testDir/file1.txt")
    touch(s"$testDir/file2.txt")
    
    // List directory contents
    val files = ls(fs, testDir)
    
    // Should have 2 files
    assert(files.length == 2)
    
    // Verify files exist
    val fileNames = files.map(_.getPath.getName)
    assert(fileNames.contains("file1.txt"))
    assert(fileNames.contains("file2.txt"))
    
    // Cleanup
    rm.r(testDir)
  }

  it should "throw FileNotFoundException when directory does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val nonExistentDir = "nonexistent_dir"
    
    assertThrows[FileNotFoundException] {
      ls(fs, nonExistentDir)
    }
  }

  it should "throw IOException when path is not a directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val testFile = "test_file.txt"
    
    // Create file
    touch(testFile)
    
    assertThrows[IOException] {
      ls(fs, testFile)
    }
    
    // Cleanup
    rm(testFile)
  }

  it should "list directory contents with filter" in {
    implicit val fs = clusterTest.getFileSystem()
    val testDir = "test_ls_filter_dir"
    
    // Create directory with files
    mkdir(testDir)
    touch(s"$testDir/file1.txt")
    touch(s"$testDir/file2.log")
    touch(s"$testDir/file3.txt")
    
    // Create filter for .txt files only
    val txtFilter = new PathFilter {
      def accept(path: Path): Boolean = path.getName.endsWith(".txt")
    }
    
    // List filtered directory contents
    val files = ls(fs, testDir, txtFilter)
    
    // Should have 2 .txt files
    assert(files.length == 2)
    
    // Verify only .txt files are returned
    val fileNames = files.map(_.getPath.getName)
    assert(fileNames.contains("file1.txt"))
    assert(fileNames.contains("file3.txt"))
    assert(!fileNames.contains("file2.log"))
    
    // Cleanup
    rm.r(testDir)
  }
}

@DoNotDiscover
class TestLsDetails
    extends AnyFlatSpec
    with MiniHDFSRunnerCoreOps
    with should.Matchers {

  "ls.details" should "list directory contents with detailed information" in {
    implicit val fs = clusterTest.getFileSystem()
    val testDir = "test_ls_details_dir"
    
    // Create directory with a file
    mkdir(testDir)
    touch(s"$testDir/test_file.txt")
    
    // Add content to file
    val outputStream = fs.create(new Path(s"$testDir/test_file.txt"))
    outputStream.write("test content".getBytes)
    outputStream.close()
    
    // List directory with details
    val details = ls.details(fs, testDir)
    
    // Should contain file information
    assert(details.contains("test_file.txt"))
    assert(details.length > 0)
    
    // Cleanup
    rm.r(testDir)
  }
}

@DoNotDiscover
class TestCat
    extends AnyFlatSpec
    with MiniHDFSRunnerCoreOps
    with should.Matchers {

  "cat" should "display file contents" in {
    implicit val fs = clusterTest.getFileSystem()
    val testFile = "test_cat_file.txt"
    val content = "Hello, World!\nThis is a test file."
    
    // Create file with content
    touch(testFile)
    val outputStream = fs.create(new Path(testFile))
    outputStream.write(content.getBytes)
    outputStream.close()
    
    // Read file contents
    val readContent = cat(fs, testFile)
    
    // Verify content matches
    assert(readContent == content)
    
    // Cleanup
    rm(testFile)
  }

  it should "throw FileNotFoundException when file does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val nonExistentFile = "nonexistent_file.txt"
    
    assertThrows[FileNotFoundException] {
      cat(fs, nonExistentFile)
    }
  }

  it should "throw IOException when path is not a file" in {
    implicit val fs = clusterTest.getFileSystem()
    val testDir = "test_cat_dir"
    
    // Create directory
    mkdir(testDir)
    
    assertThrows[IOException] {
      cat(fs, testDir)
    }
    
    // Cleanup
    rm.r(testDir)
  }

  it should "display file contents with line numbers" in {
    implicit val fs = clusterTest.getFileSystem()
    val testFile = "test_cat_numbered.txt"
    val content = "Line 1\nLine 2\nLine 3"
    
    // Create file with content
    touch(testFile)
    val outputStream = fs.create(new Path(testFile))
    outputStream.write(content.getBytes)
    outputStream.close()
    
    // Read file contents with line numbers
    val numberedContent = cat.numbered(fs, testFile)
    
    // Verify line numbers are added
    assert(numberedContent.contains("   1: Line 1"))
    assert(numberedContent.contains("   2: Line 2"))
    assert(numberedContent.contains("   3: Line 3"))
    
    // Cleanup
    rm(testFile)
  }

  it should "display first N lines of file" in {
    implicit val fs = clusterTest.getFileSystem()
    val testFile = "test_cat_head.txt"
    val content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
    
    // Create file with content
    touch(testFile)
    val outputStream = fs.create(new Path(testFile))
    outputStream.write(content.getBytes)
    outputStream.close()
    
    // Read first 3 lines
    val headContent = cat.head(fs, testFile, 3)
    
    // Verify only first 3 lines are returned
    assert(headContent == "Line 1\nLine 2\nLine 3")
    
    // Cleanup
    rm(testFile)
  }

  it should "display last N lines of file" in {
    implicit val fs = clusterTest.getFileSystem()
    val testFile = "test_cat_tail.txt"
    val content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
    
    // Create file with content
    touch(testFile)
    val outputStream = fs.create(new Path(testFile))
    outputStream.write(content.getBytes)
    outputStream.close()
    
    // Read last 3 lines
    val tailContent = cat.tail(fs, testFile, 3)
    
    // Verify only last 3 lines are returned
    assert(tailContent == "Line 3\nLine 4\nLine 5")
    
    // Cleanup
    rm(testFile)
  }
}