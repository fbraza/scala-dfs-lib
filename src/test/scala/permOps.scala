import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import org.apache.hadoop.hdfs.{MiniDFSCluster, DistributedFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataOutputStream}
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.util.Progressable
import java.io.{FileNotFoundException, IOException}
import dfs.{chown, chmod}

// Trait to create mini hadoop cluster for permOps tests
trait MiniHDFSRunnerPermOps extends TestSuite with BeforeAndAfterAll {
  protected var clusterPermOps: MiniDFSCluster = _
  protected var testFilePath: String = _
  protected var testDirPath: String = _
  protected var testNestedDirPath: String = _
  protected var testNestedFilePath: String = _

  // Spin up a mock Hadoop cluster before every tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    clusterPermOps = spinUpMiniClusterPermOps()

    // Create test file and directory in HDFS
    implicit val fs = clusterPermOps.getFileSystem()
    testFilePath = "/test_file.txt"
    testDirPath = "/test_dir"
    testNestedDirPath = "/test_dir/nested_dir"
    testNestedFilePath = "/test_dir/nested_dir/nested_file.txt"

    // Create test file with content
    val output = fs.create(new Path(testFilePath))
    output.writeBytes("Hello, World!")
    output.close()

    // Create test directory structure
    fs.mkdirs(new Path(testDirPath))
    fs.mkdirs(new Path(testNestedDirPath))
    
    // Create nested file
    val nestedOutput = fs.create(new Path(testNestedFilePath))
    nestedOutput.writeBytes("Nested file content")
    nestedOutput.close()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    clusterPermOps.shutdown()
  }

  /** function defined to set configuration of the cluster on build it
    * @return
    *   a miniDFSCluster
    */
  private def spinUpMiniClusterPermOps(): MiniDFSCluster = {
    val config = new Configuration()
    val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
    return cluster.build()
  }
}

@DoNotDiscover
class TestChown
    extends AnyFlatSpec
    with MiniHDFSRunnerPermOps
    with should.Matchers {
  
  "chown" should "change file owner" in {
    implicit val fs = clusterPermOps.getFileSystem()
    val newOwner = "testuser"
    
    // Get original owner
    val originalStatus = fs.getFileStatus(new Path(testFilePath))
    val originalOwner = originalStatus.getOwner
    
    // Change owner
    chown(fs, testFilePath, newOwner)
    
    // Verify owner changed
    val newStatus = fs.getFileStatus(new Path(testFilePath))
    newStatus.getOwner shouldBe newOwner
    newStatus.getGroup shouldBe originalStatus.getGroup // Group should remain unchanged
  }

  it should "change file owner and group" in {
    implicit val fs = clusterPermOps.getFileSystem()
    val newOwner = "testuser"
    val newGroup = "testgroup"
    
    // Change owner and group
    chown(fs, testFilePath, newOwner, newGroup)
    
    // Verify both changed
    val status = fs.getFileStatus(new Path(testFilePath))
    status.getOwner shouldBe newOwner
    status.getGroup shouldBe newGroup
  }

  it should "change directory owner" in {
    implicit val fs = clusterPermOps.getFileSystem()
    val newOwner = "testuser"
    
    // Change directory owner
    chown(fs, testDirPath, newOwner)
    
    // Verify directory owner changed
    val status = fs.getFileStatus(new Path(testDirPath))
    status.getOwner shouldBe newOwner
  }

  it should "throw FileNotFoundException for non-existent file" in {
    implicit val fs = clusterPermOps.getFileSystem()
    intercept[FileNotFoundException] {
      chown(fs, "/non/existent/file.txt", "testuser")
    }
  }

  "chown.recursive" should "change owner for directory and all contents" in {
    implicit val fs = clusterPermOps.getFileSystem()
    val newOwner = "testuser"
    val newGroup = "testgroup"
    
    // Change ownership recursively
    chown.r(fs, testDirPath, newOwner, newGroup)
    
    // Verify directory ownership changed
    val dirStatus = fs.getFileStatus(new Path(testDirPath))
    dirStatus.getOwner shouldBe newOwner
    dirStatus.getGroup shouldBe newGroup
    
    // Verify nested directory ownership changed
    val nestedDirStatus = fs.getFileStatus(new Path(testNestedDirPath))
    nestedDirStatus.getOwner shouldBe newOwner
    nestedDirStatus.getGroup shouldBe newGroup
    
    // Verify nested file ownership changed
    val nestedFileStatus = fs.getFileStatus(new Path(testNestedFilePath))
    nestedFileStatus.getOwner shouldBe newOwner
    nestedFileStatus.getGroup shouldBe newGroup
  }

  it should "change only owner for directory and all contents" in {
    implicit val fs = clusterPermOps.getFileSystem()
    val newOwner = "anotheruser"
    
    // Get original group
    val originalStatus = fs.getFileStatus(new Path(testDirPath))
    val originalGroup = originalStatus.getGroup
    
    // Change only owner recursively
    chown.r(fs, testDirPath, newOwner)
    
    // Verify owner changed but group remained
    val dirStatus = fs.getFileStatus(new Path(testDirPath))
    dirStatus.getOwner shouldBe newOwner
    dirStatus.getGroup shouldBe originalGroup
    
    // Verify nested directory also changed
    val nestedDirStatus = fs.getFileStatus(new Path(testNestedDirPath))
    nestedDirStatus.getOwner shouldBe newOwner
    nestedDirStatus.getGroup shouldBe originalGroup
  }

  it should "throw FileNotFoundException for non-existent directory" in {
    implicit val fs = clusterPermOps.getFileSystem()
    intercept[FileNotFoundException] {
      chown.r(fs, "/non/existent/dir", "testuser")
    }
  }

  it should "work on single file (non-directory)" in {
    implicit val fs = clusterPermOps.getFileSystem()
    val newOwner = "singleuser"
    
    // Apply recursive to single file
    chown.r(fs, testFilePath, newOwner)
    
    // Verify file ownership changed
    val status = fs.getFileStatus(new Path(testFilePath))
    status.getOwner shouldBe newOwner
  }
}