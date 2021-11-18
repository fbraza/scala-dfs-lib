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
import dfs.mv

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
  "it" should "return true when file created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file01.txt"
    val isSuccess = dfs.touch(fs = fs, path = pathFile)
    assert(isSuccess)
  }

  "it" should "return true if file exists and 'overwrite' is true" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file02.txt"
    dfs.touch(fs = fs, path = pathFile)
    val isSuccess = dfs.touch(fs = fs, path = pathFile, overwrite = true)
    assert(isSuccess)
  }

  "it" should "return false if file exists and 'overwrite' is false" in {
    val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file03.txt"
    dfs.touch(fs = fs, path = pathFile)
    val isSuccess = dfs.touch(fs = fs, path = pathFile, overwrite = false)
    assert(!isSuccess)
  }

  "it" should "return false if one of the parent is a file" in {
    val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file04.txt"
    val pathFile2 = "parent/directory/test_file04.txt/test_file05.txt"
    dfs.touch(fs = fs, path = pathFile1)
    val isSuccess = dfs.touch(fs = fs, path = pathFile2)
    assert(!isSuccess)
  }
}

@DoNotDiscover
class TestMkdir extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "it" should "return true when directory created at indicated path" in {
    val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy01/"
    val isSuccess = dfs.mkdir(fs, pathDir)
    assert(isSuccess)
  }

  "it" should "return false if directory already exists" in {
    val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy02/"
    dfs.mkdir(fs, pathDir)
    val isSuccess = dfs.mkdir(fs, pathDir)
    assert(!isSuccess)
  }

  "it" should "return false if one of the parent is a file" in {
    val fs = clusterTest.getFileSystem()
    val filePath = "my/dir/file.txt"
    val newDir = "/newDir"
    dfs.touch(fs = fs, path = filePath, overwrite = false)
    val isSuccess = dfs.mkdir(fs, filePath+newDir)
    assert(!isSuccess)
  }
}

@DoNotDiscover
class TestMv extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "if" should "return false if the destination is a descendant of the source" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/"
    val destDir = "source/dir/host/home/"
    dfs.mkdir(fs = fs, path = destDir)
    val isSuccess = dfs.mv(fs, from = sourceFile, to = destDir)
    assert(!isSuccess)
  }

  "it" should "return false if the source does not exist" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/where/is_file_to_move.txt"
    val destDir = "dst/could/be/any/dir/"
    dfs.mkdir(fs = fs, path = destDir)
    val isSuccess = dfs.mv(fs, from = sourceFile, to = destDir)
    assert(!isSuccess)
  }

  "it" should "return false if a file already exists at destination path" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/is/random/my_file_to_move.txt"
    val dest = "dst/is/another/dir/file_already_present.txt"
    dfs.touch(fs = fs, path = sourceFile, overwrite = false)
    dfs.touch(fs = fs, path = dest, overwrite = false)
    val isSuccess = dfs.mv(fs, from = sourceFile, to = dest)
    assert(!isSuccess)
  }

  "it" should "return false if one of the parent for destination is a file" in {
    val fs = clusterTest.getFileSystem()
    val source = "dst/yet/another/dir/"
    val file = "dst/yet/another/file.txt"
    val dest = "dst/yet/another/file.txt/in/the/middle"
    dfs.touch(fs = fs, path = file, overwrite = false)
    val isSuccess = dfs.mv(fs, from = source, to = dest)
    assert(!isSuccess)
  }

  "it" should "return true when moving a file / folder into an existing or absent directory" in {
    val fs = clusterTest.getFileSystem()
    // first scenario
    val file = "dst/yet/another/file.txt"
    dfs.touch(fs =fs, path = file, overwrite = false)
    val existingDestDir = "dst/could/be/any/"
    dfs.mkdir(fs = fs, path = existingDestDir)
    val nonExistingDestDir = "dst/could/be/any/new/dir/"
    val isSuccess1 = mv(fs = fs, from = file, to = existingDestDir)
    assert(isSuccess1)
    // second scenario
    val movedFile = "dst/could/be/any/file.txt"
    val isSuccess2 = mv(fs = fs, from = movedFile, to = nonExistingDestDir)
    assert(isSuccess2)
  }
}


class TestDistributor extends Stepwise(
  Sequential(new TestTouch, new TestMkdir, new TestMv)
)

// BLOCKSIZE: 134217728
// BLOCKSIZE: fbraza
// BLOCKSIZE: -rw-r--r--
// BLOCKSIZE: 1
