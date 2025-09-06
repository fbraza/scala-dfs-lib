import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import org.apache.hadoop.hdfs.{MiniDFSCluster, DistributedFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataOutputStream}
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.util.Progressable
import java.io.FileNotFoundException
import dfs.{size, replication, blockSize, getPath, stat}

// Trait to create mini hadoop cluster for statOps tests
trait MiniHDFSRunnerStatOps extends TestSuite with BeforeAndAfterAll {
  protected var clusterStatOps: MiniDFSCluster = _
  protected var testFilePath: String = _
  protected var testDirPath: String = _

  // Spin up a mock Hadoop cluster before every tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    clusterStatOps = spinUpMiniClusterStatOps()

    // Create test file and directory in HDFS
    implicit val fs = clusterStatOps.getFileSystem()
    testFilePath = "/test_file.txt"
    testDirPath = "/test_dir"

    // Create test file with content
    val output = fs.create(new Path(testFilePath))
    output.writeBytes("Hello, World!")
    output.close()

    // Create test directory
    fs.mkdirs(new Path(testDirPath))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    clusterStatOps.shutdown()
  }

  /** function defined to set configuration of the cluster on build it
    * @return
    *   a miniDFSCluster
    */
  private def spinUpMiniClusterStatOps(): MiniDFSCluster = {
    val config = new Configuration()
    val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
    return cluster.build()
  }
}

@DoNotDiscover
class TestSize
    extends AnyFlatSpec
    with MiniHDFSRunnerStatOps
    with should.Matchers {
  "size" should "return correct file size" in {
    implicit val fs = clusterStatOps.getFileSystem()
    val fileSize = dfs.size(testFilePath)
    fileSize should be > 0L
  }

  it should "throw FileNotFoundException for non-existent file" in {
    implicit val fs = clusterStatOps.getFileSystem()
    intercept[FileNotFoundException] {
      dfs.size("/non/existent/file.txt")
    }
  }
}

@DoNotDiscover
class TestReplication
    extends AnyFlatSpec
    with MiniHDFSRunnerStatOps
    with should.Matchers {
  "replication" should "return replication factor" in {
    implicit val fs = clusterStatOps.getFileSystem()
    val replication = dfs.replication(testFilePath)
    replication should be >= 0.toShort
  }

  it should "throw FileNotFoundException for non-existent file" in {
    implicit val fs = clusterStatOps.getFileSystem()
    intercept[FileNotFoundException] {
      dfs.replication("/non/existent/file.txt")
    }
  }
}

@DoNotDiscover
class TestBlockSize
    extends AnyFlatSpec
    with MiniHDFSRunnerStatOps
    with should.Matchers {
  "blockSize" should "return block size" in {
    implicit val fs = clusterStatOps.getFileSystem()
    val blockSize = dfs.blockSize(testFilePath)
    blockSize should be > 0L
  }

  it should "throw FileNotFoundException for non-existent file" in {
    implicit val fs = clusterStatOps.getFileSystem()
    intercept[FileNotFoundException] {
      dfs.blockSize("/non/existent/file.txt")
    }
  }
}

@DoNotDiscover
class TestGetPath
    extends AnyFlatSpec
    with MiniHDFSRunnerStatOps
    with should.Matchers {
  "getPath" should "return normalized path" in {
    implicit val fs = clusterStatOps.getFileSystem()
    val normalizedPath = dfs.getPath(testFilePath)
    normalizedPath should not be empty
  }

  it should "work with non-existent file paths" in {
    implicit val fs = clusterStatOps.getFileSystem()
    val normalizedPath = dfs.getPath("/non/existent/file.txt")
    normalizedPath shouldBe "/non/existent/file.txt"
  }
}

@DoNotDiscover
class TestStat
    extends AnyFlatSpec
    with MiniHDFSRunnerStatOps
    with should.Matchers {
  "stat" should "return complete FileMetadata" in {
    implicit val fs = clusterStatOps.getFileSystem()
    val metadata = dfs.stat(testFilePath)
    metadata shouldBe a[dfs.stat.FileMetadata]
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
    implicit val fs = clusterStatOps.getFileSystem()
    val metadata = dfs.stat(testDirPath)
    metadata.isFile shouldBe false
    metadata.isDirectory shouldBe true
  }

  it should "throw FileNotFoundException for non-existent file" in {
    implicit val fs = clusterStatOps.getFileSystem()
    intercept[FileNotFoundException] {
      dfs.stat("/non/existent/file.txt")
    }
  }
}

// Removed individual distributor - using MasterTestSuite instead
