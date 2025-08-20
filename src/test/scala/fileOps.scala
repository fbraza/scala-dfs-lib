import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._
import org.apache.hadoop.hdfs.{MiniDFSCluster, DistributedFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataOutputStream}
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.util.Progressable
import dfs.{mkdir, mv, touch, exists, rm}

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
    val isCreated = touch(path = pathFile)
    assert(isCreated)
  }

  "it" should "return true if file exists and 'overwrite' is true" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file02.txt"
    touch(path = pathFile)
    val isCreated = touch(path = pathFile, overwrite = true)
    assert(isCreated)
  }

  "it" should "return false if file exists and 'overwrite' is false" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathFile = "parent/directory/test_file03.txt"
    touch(path = pathFile)
    val isCreated = touch(path = pathFile, overwrite = false)
    assert(!isCreated)
  }

  "it" should "return false if one of the parent is a file" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathFile1 = "parent/directory/test_file04.txt"
    val pathFile2 = "parent/directory/test_file04.txt/test_file05.txt"
    touch(path = pathFile1)
    val isCreated = touch(path = pathFile2)
    assert(!isCreated)
  }
}

@DoNotDiscover
class TestMkdir extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

  "it" should "return true when directory created at indicated path" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy01/"
    val isCreated = mkdir(path = pathDir)
    assert(isCreated)
  }

  "it" should "return false if directory already exists" in {
    implicit val fs = clusterTest.getFileSystem()
    val pathDir = "my/test/directoy02/"
    mkdir(pathDir)
    val isCreated = mkdir(pathDir)
    assert(!isCreated)
  }

  "it" should "return false if one of the parent is a file" in {
    implicit val fs = clusterTest.getFileSystem()
    val filePath = "my/dir/file.txt"
    val newDir = "/newDir"
    touch(path = filePath, overwrite = false)
    val isCreated = mkdir(filePath+newDir)
    assert(!isCreated)
  }
}

@DoNotDiscover
class TestMv extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

  "it" should "return false if the destination is a descendant of the source" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "src/dir/"
    val destDir = "src/dir/host/home/"
    mkdir(path = destDir)
    val isMoved = mv(from = sourceFile, to = destDir)
    assert(!isMoved)
  }

  "it" should "return false if the source does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "src/dir/where/is_file_to_move.txt"
    val destDir = "dst/could/be/any/dir/"
    mkdir(path = destDir)
    val isMoved = mv(from = sourceFile, to = destDir)
    assert(!isMoved)
  }

  "it" should "return false if a file already exists at destination path" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "src/is/random/my_file_to_move.txt"
    val dest = "dst/is/another/dir/file_already_present.txt"
    touch(path = sourceFile, overwrite = false)
    touch(path = dest, overwrite = false)
    val isMoved = mv(from = sourceFile, to = dest)
    assert(!isMoved)
  }

  "it" should "return false if one of the parent directory does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val path = "dst/again/a/new/folder"
    val source = "dst/again/a/new/file.txt"
    mkdir(path = path)
    touch(path = source)
    val unexistingParent1 = "dst/again/a/new/folder/not/here"
    val unexistingParent2 = "dst/again/a/old/folder"
    val isMovedScenario1 = mv(from = source, to = path)
    val isMovedScenario2 = !mv(from = source, to = unexistingParent1)
    val isMovedScenario3 = !mv(from = source, to = unexistingParent2)
    assert(isMovedScenario1 && isMovedScenario2 && isMovedScenario3)
  }

  "it" should "return false if one of the parent for destination is a file" in {
    implicit val fs = clusterTest.getFileSystem()
    val source = "dst/yet/another/dir/"
    val file = "dst/yet/another/file.txt"
    val dest = "dst/yet/another/file.txt/in/the/middle"
    touch(path = file, overwrite = false)
    val isMoved = mv(from = source, to = dest)
    assert(!isMoved)
  }

  "it" should "return false if destination does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "dst/yet/another/source/file_to_move_there.txt"
    val dstDir = "inexsitent/dst/dir/to/fill/"
    touch(path = sourceFile)
    val isMoved = !mv(from = sourceFile, to = dstDir)
    assert(isMoved)
  }

  "it" should "return true when a file / folder is moved into destination" in {
    implicit val fs = clusterTest.getFileSystem()
    val sourceFile = "dst/yet/another/source/file.txt"
    val sourceDir = "dst/yet/another/source/dir/"
    val destination = "dst/yet/another/destination/dir/"
    touch(path = sourceFile)
    mkdir(path = sourceDir)
    mkdir(path = destination)
    val isFileMoved = mv(from = sourceFile, to = destination)
    val isDirMoved  = mv(from = sourceDir, to = destination)
    assert(isFileMoved && isDirMoved)
  }
}

@DoNotDiscover
class TestMvInto extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

  "it" should "return false if destination is not a directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val source = "dst/yet/another/source/file/to/move.txt"
    val destination = "dst/yet/another/source/file/to/moved.txt"
    touch(path = source)
    touch(path = destination)
    val isMoved = mv.into(from = source, to = destination)
    assert(!isMoved)
  }

  "it" should "create missing parent directories and move the source to the newly created path" in {
    implicit val fs = clusterTest.getFileSystem()
    val source = "src/my/file/to_move.txt"
    val destination = "dst/my/destination/"
    touch(path = source)
    val isThere1 = exists(path = destination)
    val isMoved  = mv.into(from = source, to = destination)
    val isThere2 = exists(path = destination)
    assert(!isThere1 && isMoved && isThere2)
  }
}

@DoNotDiscover
class TestMvOver extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

  "it" should "move a file and overwrite an existing file" in {
    implicit val fs = clusterTest.getFileSystem()
    val from = "/from.txt"
    val to = "/to.txt"
    touch(from) should be(true)
    touch(to) should be(true)
    mv.over(from, to) should be(true)
    fs.exists(new Path(from)) should be(false)
    fs.exists(new Path(to)) should be(true)
  }

  "it" should "move a directory and overwrite an existing directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val from = "/from_dir"
    val to = "/to_dir"
    mkdir(from) should be(true)
    touch(s"$from/file.txt") should be(true)
    mkdir(to) should be(true)
    touch(s"$to/old_file.txt") should be(true)

    mv.over(from, to) should be(true)
    fs.exists(new Path(from)) should be(false)
    fs.exists(new Path(to)) should be(true)
    fs.exists(new Path(s"$to/file.txt")) should be(true)
    fs.exists(new Path(s"$to/old_file.txt")) should be(false)
  }

  "it" should "move a file to a path with non-existent parents" in {
    implicit val fs = clusterTest.getFileSystem()
    val from = "/move_from.txt"
    val to = "/new/parent/move_to.txt"
    touch(from) should be(true)
    mv.over(from, to) should be(true)
    fs.exists(new Path(from)) should be(false)
    fs.exists(new Path(to)) should be(true)
  }
}

@DoNotDiscover
class TestRm extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

  "it" should "return false if target is a directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val target = "directory/to/delete/"
    mkdir(path = target)
    val isDeleted = rm(path = target)
    assert(!isDeleted)
  }

  "it" should "return false if target does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val target = "file/to/delete/do_not_exist.txt"
    val isDeleted = rm(path = target)
    assert(!isDeleted)
  }

  "it" should "return true when deleting an existing file" in {
    implicit val fs = clusterTest.getFileSystem()
    val target = "file/to/delete/do_not_exist.txt"
    touch(path = target)
    val isDeleted = rm(path = target)
    assert(isDeleted)
  }
}

@DoNotDiscover
class TestRmr extends AnyFlatSpec with miniHDFSRunner with should.Matchers {

  "it" should "return false if target does not exist" in {
    implicit val fs = clusterTest.getFileSystem()
    val target = "directory/to/delete/does/not/exist"
    val isDeleted = rm.r(path = target)
    assert(!isDeleted)
  }

  "it" should "return false if target is one of root directories" in {
    implicit val fs = clusterTest.getFileSystem()
    val target = "/usr/my/directory"
    mkdir(path = target)
    val isDeleted = rm.r(path = target)
    assert(!isDeleted)
  }

  "it" should "recursively remove a directory" in {
    implicit val fs = clusterTest.getFileSystem()
    val dir = "/dir_to_remove"
    mkdir(s"$dir/subdir") should be(true)
    touch(s"$dir/subdir/file.txt") should be(true)
    rm.r(dir) should be(true)
    fs.exists(new Path(dir)) should be(false)
  }

  "it" should "not remove root directory" in {
    implicit val fs = clusterTest.getFileSystem()
    rm.r("/") should be(false)
  }
}

class TestDistributor extends Stepwise(
  Sequential(
  new TestTouch,
  new TestMkdir,
  new TestMv,
  new TestMvInto,
  new TestMvOver,
  new TestRm,
  new TestRmr
  )
)
