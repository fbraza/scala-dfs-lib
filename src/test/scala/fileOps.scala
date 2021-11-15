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
  "Renaming an existing file" should "work" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/source_file01.txt"
    val destDir = "source/dir/"
    val fileNewName = "new_source_file01.txt"
    dfs.touch(fs, sourceFile, false)
    assert(dfs.mv(fs = fs, from = sourceFile, to = destDir+fileNewName))
  }

  "Moving an existing file to an existing directory" should "work" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/source_file02.txt"
    val destDir = "dest/dir02/"
    val fileName = "source_file02.txt"
    dfs.touch(fs = fs, path = sourceFile, overwrite = false)
    dfs.mkdir(fs = fs, path = destDir)
    assert(dfs.mv(fs = fs, from = sourceFile, to = destDir+fileName))
    assert(dfs.exists(fs = fs, path = destDir+fileName))
  }

  "Moving an existing file to an non-existing directory" should "not work" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/source_file03.txt"
    val destDir = "dest/dir03/"
    val fileName = "source_file03.txt"
    dfs.touch(fs = fs, path = sourceFile, overwrite = false)
    assert(!dfs.mv(fs = fs, from = sourceFile, to = destDir+fileName))
  }

  "Moving a non-existing file to an existing directory" should "not work" in {
    val fs = clusterTest.getFileSystem()
    val sourceFile = "source/dir/source_file04.txt"
    val destDir = "dest/dir04/"
    dfs.mkdir(fs = fs, path = destDir)
    assert(!dfs.mv(fs = fs, from = sourceFile, to = destDir))
  }
}


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
  Sequential(
  new TestTouch,
  new TestMkdir
  )
)

// BLOCKSIZE: 134217728
// BLOCKSIZE: fbraza
// BLOCKSIZE: -rw-r--r--
// BLOCKSIZE: 1
