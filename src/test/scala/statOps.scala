import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.io.FileNotFoundException
import dfs.{size, replication, blockSize, getPath, stat}

trait TestSetup {
  implicit val fs: FileSystem = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:///")
    FileSystem.get(conf)
  }

  // Create a temporary test file
  protected val testFile = File.createTempFile("test", ".txt")
  testFile.deleteOnExit()
  new java.io.PrintWriter(testFile) { write("Hello, World!"); close() }

  // Create a temporary test directory
  protected val testDir = new File(File.createTempFile("testdir", "").getParentFile, "testdir")
  testDir.mkdir()
  testDir.deleteOnExit()
}

class TestSize extends AnyFlatSpec with Matchers with TestSetup {
  "size" should "return correct file size" in {
    val fileSize = dfs.size(testFile.getAbsolutePath)
    fileSize should be > 0L
  }

  it should "throw FileNotFoundException for non-existent file" in {
    intercept[FileNotFoundException] {
      dfs.size("/non/existent/file.txt")
    }
  }
}

class TestReplication extends AnyFlatSpec with Matchers with TestSetup {
  "replication" should "return replication factor" in {
    val replication = dfs.replication(testFile.getAbsolutePath)
    replication should be >= 0.toShort
  }

  it should "throw FileNotFoundException for non-existent file" in {
    intercept[FileNotFoundException] {
      dfs.replication("/non/existent/file.txt")
    }
  }
}

class TestBlockSize extends AnyFlatSpec with Matchers with TestSetup {
  "blockSize" should "return block size" in {
    val blockSize = dfs.blockSize(testFile.getAbsolutePath)
    blockSize should be > 0L
  }

  it should "throw FileNotFoundException for non-existent file" in {
    intercept[FileNotFoundException] {
      dfs.blockSize("/non/existent/file.txt")
    }
  }
}

class TestGetPath extends AnyFlatSpec with Matchers with TestSetup {
  "getPath" should "return normalized path" in {
    val normalizedPath = dfs.getPath(testFile.getAbsolutePath)
    normalizedPath should not be empty
  }

  it should "work with non-existent file paths" in {
    val normalizedPath = dfs.getPath("/non/existent/file.txt")
    normalizedPath shouldBe "/non/existent/file.txt"
  }
}

class TestStat extends AnyFlatSpec with Matchers with TestSetup {
  "stat" should "return complete FileMetadata" in {
    val metadata = dfs.stat(testFile.getAbsolutePath)
    metadata shouldBe a [dfs.stat.FileMetadata]
    metadata.path should not be empty
    metadata.size should be > 0L
    metadata.isFile shouldBe true
    metadata.isDirectory shouldBe false
    metadata.modificationTime should be > 0L
    metadata.accessTime should be >= 0L
    metadata.owner should not be empty
    metadata.group should not be empty
    metadata.permissions should not be empty
    metadata.replication should be >= 0.toShort
    metadata.blockSize should be > 0L
  }

  it should "return correct metadata for directory" in {
    val metadata = dfs.stat(testDir.getAbsolutePath)
    metadata.isFile shouldBe false
    metadata.isDirectory shouldBe true
  }

  it should "throw FileNotFoundException for non-existent file" in {
    intercept[FileNotFoundException] {
      dfs.stat("/non/existent/file.txt")
    }
  }
}