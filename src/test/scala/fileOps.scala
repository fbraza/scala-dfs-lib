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
@DoNotDiscover
class TestTouch extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "File" should "be created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file01.txt"
    dfs.touch(fs = fs, path = pathFile, overwrite = false)
    assert(dfs.exists(fs = fs, path = pathFile))
    assert(dfs.isFile(fs = fs, path = pathFile))
  }

  "If overwrite is set to true and file exists, the file" should " be overwriten" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file02"
    dfs.touch(fs = fs, path = pathFile, overwrite = false)
    assert(dfs.exists(fs = fs, path = pathFile))
    assert(dfs.isFile(fs = fs, path = pathFile))
    dfs.touch(fs = fs, path = pathFile, overwrite = true)
    assert(dfs.exists(fs = fs, path = pathFile))
    assert(dfs.isFile(fs = fs, path = pathFile))
  }

  "If overwrite is set to false and file exists, operation" should " raise an error" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file03"
    dfs.touch(fs = fs, path = pathFile, overwrite = false)
    assert(dfs.exists(fs = fs, path = pathFile))
    assert(dfs.isFile(fs = fs, path = pathFile))
    a [RemoteException] should be thrownBy dfs.touch(clusterTest.getFileSystem(), pathFile, false)
  }

  "cannotOverrides" should "return true if and only if overwrite is set to false and file exists" in {
    val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file04"
    val pathFile2 = "parent/directory/test_file05"
    val pathFile3 = "unexisting/path/test_file06"
    dfs.touch(fs = fs, path = pathFile1, overwrite = false)
    dfs.touch(fs = fs, path = pathFile2, overwrite = false)
    val cannotOverwrite1 = dfs.touch.cannotOverwrite(fs = fs, overwrite = false, path = new Path(pathFile1))
    val cannotOverwrite2 = dfs.touch.cannotOverwrite(fs = fs, overwrite = true,  path = new Path(pathFile2))
    val cannotOverwrite3 = dfs.touch.cannotOverwrite(fs = fs, overwrite = true,  path = new Path(pathFile3))
    val cannotOverwrite4 = dfs.touch.cannotOverwrite(fs = fs, overwrite = false, path = new Path(pathFile3))
    assert(dfs.exists(fs = fs, path = pathFile1))
    assert(dfs.exists(fs = fs, path = pathFile2))
    assert(cannotOverwrite1)
    assert(!cannotOverwrite2)
    assert(!cannotOverwrite3)
    assert(!cannotOverwrite4)
  }
}

  // "Directories" should "be created at indicated path" in {
  //   val fs = clusterTest.getFileSystem()
  //   val pathDir = "my/test/directoy/"
  //   assert(dfs.mkdir(fs, pathDir))
  // }

  // "Creation of directories and subsequent files" should "work at indicated path" in {
  //   val fs = clusterTest.getFileSystem()
  //   val pathDir = "my/newTest/directoy/"
  //   val pathFile1 = "my/newTest/directoy/test_file07.txt"
  //   val pathFile2 = "my/newTest/directoy/test_file08.txt"
  //   assert(dfs.mkdir(fs, pathDir))
  //   assert(dfs.touch(fs, pathFile1, true))
  //   assert(dfs.touch(fs, pathFile2, true))
  // }

  // "Moving a file to a destination folder" should "work as expected" in {
  //   val fs = clusterTest.getFileSystem()
  //   val sourceFile = "source/dir/source_file01.txt"
  //   val destDir = "destination/dir/"
  //   val fileNewName = "new_source_file01.txt"
  //   dfs.touch(fs, sourceFile, false)
  //   dfs.mkdir(fs, destDir)
  //   assert(dfs.mv(fs, sourceFile, destDir+fileNewName))
  //   assert(dfs.exists(fs, destDir+fileNewName))
  // }

  // "Moving a file to a non existing destination folder" should " not work" in {
  //   val fs = clusterTest.getFileSystem()
  //   val sourceFile = "source/dir/source_file.txt"
  //   val destDir = "destination/dir/"
  //   val fileNewName = "new_source_file.txt"
  //   // before creation
  //   assert(!dfs.mv(fs, sourceFile, destDir))
  //   // create file and directory
  //   dfs.touch(fs, sourceFile, false)
  //   assert(!dfs.mv(fs, sourceFile, destDir+fileNewName))
  // }

class TestDistributor extends Stepwise(
  Sequential(new TestTouch)
)

// BLOCKSIZE: 134217728
// BLOCKSIZE: fbraza
// BLOCKSIZE: -rw-r--r--
// BLOCKSIZE: 1
