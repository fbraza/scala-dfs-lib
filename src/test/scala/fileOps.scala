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
    val config = new Configuration
    val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
    return cluster.build()
  }
}

class TestTouch extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "Files" should "be created at indicated path " in {
    val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file01.txt"
    val pathFile2 = "parent/directory/test_file02.txt"
    val pathFile3 = "parent/directory/test_file03.txt"
    val pathFile4 = "parent/directory/test_file04.txt"
    val pathFile5 = "parent/directory/test_file05.txt"
    val pathFile6 = "parent/directory/test_file06.txt"
    dfs.touch(fs = fs, path = pathFile1)
    dfs.touch(fs = fs, path = pathFile2, overwrite = false)
    dfs.touch(fs = fs, path = pathFile3, replicationFactor = 1)
    dfs.touch(fs = fs, path = pathFile4, overwrite = false, bufferSize = 4096)
    dfs.touch(fs = fs, path = pathFile5, overwrite = false, bufferSize = 4096, replicationFactor = 1, blockSize = 134217728)
    assert(dfs.exists(fs, pathFile1))
    assert(dfs.exists(fs, pathFile2))
    assert(dfs.exists(fs, pathFile3))
    assert(dfs.exists(fs, pathFile4))
    assert(dfs.exists(fs, pathFile5))
    assert(dfs.isFile(fs, pathFile1))
    assert(dfs.isFile(fs, pathFile2))
    assert(dfs.isFile(fs, pathFile3))
    assert(dfs.isFile(fs, pathFile4))
    assert(dfs.isFile(fs, pathFile5))
  }

  "By default files" should "be overwriten at same path " in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/dummy.txt"
    dfs.touch(fs, pathFile)
    dfs.touch(fs, pathFile)
    assert(dfs.exists(fs, pathFile))
  }

  "Preventing overwrite" should "raise an error if file already exists" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/dummy.txt"
    dfs.touch(fs, pathFile)
    an [RemoteException] should be thrownBy dfs.touch(clusterTest.getFileSystem(), "parent/directory/dummy.txt", false)
  }
}


// BLOCKSIZE: 134217728
// BLOCKSIZE: fbraza
// BLOCKSIZE: rw-r--r--
// BLOCKSIZE: 1
