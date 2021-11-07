import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.util.Progressable

// Trait to create mini hadoop cluster Any test can extend from it and use the mini cluster
trait miniHDFSRunner extends TestSuite with BeforeAndAfterAll {
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
    * @return a miniDFSCluster
    */
  private def spinUpMiniCluster(): MiniDFSCluster = {
    val config = new Configuration()
    val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
    return cluster.build()
  }
}

class TestFileAndFolderOps extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "Files" should "be created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file01.txt"
    val pathFile2 = "parent/directory/test_file02.txt"
    assert(dfs.touch(fs, pathFile1, true))
    assert(dfs.touch(fs, pathFile2, true))
  }

  "If overwrite is true, the file" should "be overwriten" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file03"
    assert(dfs.touch(fs, pathFile, true))
    assert(dfs.touch(fs, pathFile, true))
  }

  "If overwrite is false and path does not exist, the file" should "be created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file04.txt"
    val pathFile2 = "parent/directory/test_file05.txt"
    assert(dfs.touch(fs, pathFile1, false))
    assert(dfs.touch(fs, pathFile2, false))
  }

  "If overwrite is false and path exists, the file" should "not be created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file06.txt"
    assert(dfs.touch(fs, pathFile, false))
    assert(!dfs.touch(fs, pathFile, false))
  }

  "Directories" should "be created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy/"
    assert(dfs.mkdir(fs, pathDir))
  }

  "Creation of directories and subsequent files" should "work at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathDir = "my/newTest/directoy/"
    val pathFile1 = "my/newTest/directoy/test_file07.txt"
    val pathFile2 = "my/newTest/directoy/test_file08.txt"
    assert(dfs.mkdir(fs, pathDir))
    assert(dfs.touch(fs, pathFile1, true))
    assert(dfs.touch(fs, pathFile2, true))
  }
}

// BLOCKSIZE: 134217728
// BLOCKSIZE: fbraza
// BLOCKSIZE: -rw-r--r--
// BLOCKSIZE: 1
