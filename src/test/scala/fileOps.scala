import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import org.apache.hadoop.hdfs.{MiniDFSCluster, DistributedFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataOutputStream}
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
  "it" should "return true when file created at indicated path" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file01.txt"
    val isSuccess = dfs.touch(path = pathFile)
    assert(isSuccess)
  }

  "it" should "return true if file exists and 'overwrite' is true" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file02.txt"
    dfs.touch(path = pathFile)
    val isSuccess = dfs.touch(path = pathFile, overwrite = true)
    assert(isSuccess)
  }

  "it" should "return false if file exists and 'overwrite' is false" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file03.txt"
    dfs.touch(path = pathFile)
    val isSuccess = dfs.touch(path = pathFile, overwrite = false)
    assert(!isSuccess)
  }

  "it" should "return false if one of the parent is a file" in {
    implicit implicit val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file04.txt"
    val pathFile2 = "parent/directory/test_file04.txt/test_file05.txt"
    dfs.touch(path = pathFile1)
    val isSuccess = dfs.touch(path = pathFile2)
    assert(!isSuccess)
  }
}

@DoNotDiscover
class TestMkdir extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "it" should "return true when directory created at indicated path" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy01/"
    val isSuccess = dfs.mkdir(path = pathDir)
    assert(isSuccess)
  }

  "it" should "return false if directory already exists" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy02/"
    dfs.mkdir(pathDir)
    val isSuccess = dfs.mkdir(pathDir)
    assert(!isSuccess)
  }

  "it" should "return false if one of the parent is a file" in {
    implicit val fs = clusterTest.getFileSystem()
    val filePath = "my/dir/file.txt"
    val newDir = "/newDir"
    dfs.touch(path = filePath, overwrite = false)
    val isSuccess = dfs.mkdir(filePath+newDir)
    assert(!isSuccess)
  }
}

@DoNotDiscover
class TestMv extends AnyFlatSpec with miniHDFSRunner with should.Matchers {
  "if" should "return false if the destination is a descendant of the source" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/"
    val destDir = "source/dir/host/home/"
    dfs.mkdir(path = destDir)
    val isSuccess = dfs.mv(from = sourceFile, to = destDir)
    assert(!isSuccess)
  }

  "it" should "return false if the source does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/where/is_file_to_move.txt"
    val destDir = "dst/could/be/any/dir/"
    dfs.mkdir(path = destDir)
    val isSuccess = dfs.mv(from = sourceFile, to = destDir)
    assert(!isSuccess)
  }

  "it" should "return false if a file already exists at destination path" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "source/is/random/my_file_to_move.txt"
    val dest = "dst/is/another/dir/file_already_present.txt"
    dfs.touch(path = sourceFile, overwrite = false)
    dfs.touch(path = dest, overwrite = false)
    val isSuccess = dfs.mv(from = sourceFile, to = dest)
    assert(!isSuccess)
  }

  "it" should "return false if one of the parent directory does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val path = "dst/again/a/new/folder"
    val source = "dst/again/a/new/file.txt"
    dfs.mkdir(path = path)
    dfs.touch(path = source)
    val unexistingParent1 = "dst/again/a/new/folder/not/here"
    val unexistingParent2 = "dst/again/a/old/folder"
    assert(dfs.mv(from = source, to = path))
    assert(!dfs.mv(from = source, to = unexistingParent1))
    assert(!dfs.mv(from = source, to = unexistingParent2))
  }

  "it" should "return false if one of the parent for destination is a file" in {
    implicit val fs = clusterTest.getFileSystem()
    val source = "dst/yet/another/dir/"
    val file = "dst/yet/another/file.txt"
    val dest = "dst/yet/another/file.txt/in/the/middle"
    dfs.touch(path = file, overwrite = false)
    val isSuccess = dfs.mv(from = source, to = dest)
    assert(!isSuccess)
  }

  "it" should "return true when a file / folder is moved into destination" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "dst/yet/another/source/file.txt"
    val sourceDir = "dst/yet/another/source/dir/"
    val destination = "dst/yet/another/destination/dir/"
    dfs.touch(path = sourceFile)
    dfs.mkdir(path = sourceDir)
    dfs.mkdir(path = destination)
    assert(dfs.mv(from = sourceFile, to = destination))
    assert(dfs.mv(from = sourceDir, to = destination))
  }
}

@DoNotDiscover
class TestMvInto extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

}

@DoNotDiscover
class TestMvOver extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

}

class TestDistributor extends Stepwise(
  Sequential(new TestTouch, new TestMkdir, new TestMv)
)

// BLOCKSIZE: 134217728
// BLOCKSIZE: fbraza
// BLOCKSIZE: -rw-r--r--
// BLOCKSIZE: 1
