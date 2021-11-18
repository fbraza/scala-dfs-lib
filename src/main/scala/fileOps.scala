package dfs

import org.apache.hadoop.fs.{
  FileSystem,
  Path,
  FileStatus,
  PathFilter,
  FSDataOutputStream,
  FileAlreadyExistsException,
  ParentNotDirectoryException
}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.ipc.RemoteException
import com.typesafe.scalalogging.Logger

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration

/** Create a file to the specified path with default permissions. Create missing
  * parent directories found in the path. You can decide to (i) overwrite the
  * file, set (ii) the hadoop replication factor, (iii) the hadoop block size,
  * (iv) the writing buffer size. To create directories please prefer using
  * [[dfs.mkdir]]
  *
  * Code has been implemented to abstract away the most common errors raised by
  * the Hadoop FileSystem Java API. Errors are catched but always logged. Failed
  * files / dirs operations return false otherwise true.
  */
object touch {
  val logger = Logger("dfs.touch.scala")

  /** Create file and all its parent directories if not existing with default permissions.
    * It overwrites exisisting file when specified. Writing buffer size, replication factor
    * and block size can be specified.
    *
    * @param fs
    *   an instance of the Hadoop file system
    * @param path
    *   a string of the file absolute path
    * @param overwrite
    *   if set to true overwrite existing file
    * @param bufferSize
    *   size of the writing buffer, 4096 by default
    * @param replicationFactor
    *   Hadoop replication factor on data nodes, 1 by default
    * @param blockSize
    *   Hadoop file block size on data nodes, 134217728 bytes by default
    * @return
    *   true if operation succeeds false otherwise
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean = false,
      bufferSize: Int = 4096,
      replicationFactor: Short = 1.toShort,
      blockSize: Long = 134217728
  ): Boolean = {
    val isCreated = true
    try {
      fs.create(new Path(path), overwrite, bufferSize, replicationFactor, blockSize).close()
      logger.info(s"file created at path : $path")
      isCreated
    } catch {
      case fae: FileAlreadyExistsException =>
        logger.error(s"file at path : $path : already exists")
        !isCreated
      case pde: ParentNotDirectoryException =>
        logger.error(s"the parent path : $path : must not be a file")
        !isCreated
    }
  }
}

object mkdir {
  val logger = Logger("dfs.mkdir.scala")

  /** Create a directory and all its parents at indicated path.
    * Existing folders cannot be overwritten.
    *
    * @param fs
    *   an instance of the Hadoop file system
    * @param path
    *   absolute path of the file to be created
    * @return
    *   true if operation succeeded false otherwise
    */
  def apply(fs: FileSystem, path: String): Boolean = {
    val pathDir = new Path(path)
    var isCreated = true
    if (dfs.exists(fs, pathDir)) {
      logger.error(s"cannot create directory at path : $path : already exists")
      !isCreated
    } else {
        try {
          fs.mkdirs(pathDir) // should be true if no error caught
          logger.info(s"directory created at path : $path")
          isCreated
        } catch {
          case pde: ParentNotDirectoryException =>
            logger.error(s"the parent path : $path : must not be a file")
            !isCreated
        }
    }
  }
}

/** Rename or move files or folders to the specified path. Successful operations
  * return true otherwise false.
  */
object mv {
  val logger = Logger("dfs.mv.scala")

  /** Use to move a file or a folder to the specified path.
    *
    * @param fs
    *   an instance of the Hadoop file system
    * @param from
    *   source path
    * @param to
    *   destination path
    * @return
    *   true when process succeeds false otherwise
    */
  def apply(fs: FileSystem, from: String, to: String): Boolean = {
    val isMoved = true
    if (to.startsWith(from)) {
      logger.error(s"the destination : $to : cannot be a descendant of the source : $from ")
      !isMoved
    } else if(!dfs.exists(fs, from)) {
        logger.error(s"cannot move source : $from : because it is not found")
        !isMoved
    } else if(dfs.exists(fs, to) && dfs.isFile(fs, to)) {
        logger.error(s"cannot move : $from : to : $to : because an existing file is found the end of the destination path")
        !isMoved
    } else if(!doAllParentDirExist(fs, to)) {
        logger.error(s"cannot move : $from : to : $to : because parent folders are missing")
        !isMoved
    } else {
      val fromPath = new Path(from)
      val toPath = new Path(to)
      try {
        val moved = fs.rename(fromPath, toPath)
        logger.info(s"source : $from : moved to path : $to")
        moved
      } catch {
        case pde: ParentNotDirectoryException =>
          logger.error(s"the parent path : $to : must not be a file")
          !isMoved
      }
    }
  }

  def doAllParentDirExist(fs: FileSystem, path: String): Boolean = {
    val doesExist = true
    val pathToEvaluate = path.split("/")
    for (i <- 1 to pathToEvaluate.length) {
      if(!dfs.exists(fs, pathToEvaluate.slice(0, i).mkString("/"))) {
        return !doesExist
      }
    }
    doesExist
  }

  /** Move a file or directory inside a specified directory.
    * Create missing parent directory if any
    *
    * @param fs
    *   an instance of the Hadoop file system
    * @param from
    *   source path
    * @param to
    *   destination path
    * @return
    *   true when process succeeds false otherwise
    */
  object into {
    def apply(fs: FileSystem, from: String, to: String): Boolean = {
      val isMoved = true
      if (!dfs.isDirectory(fs, to)) {
        logger.error(s"the destination : $to : must be a directory")
        !isMoved
      } else if (!doAllParentDirExist(fs, to)) {
        mkdir(fs, to)
        mv.apply(fs, from, to)
      }
      else {
        mv.apply(fs, from, to)
      }
    }
  }

  /** Move a file or folder overwriting the destination
    *
    * @param fs
    *   an instance of the Hadoop file system
    * @param from
    *   source path
    * @param to
    *   destination path
    * @return
    *   true when process succeeds false otherwise
    */
  object over {
    def apply(fs: FileSystem, from: String, to: String): Boolean = {
      dfs.rm(fs, to, true)
      mv.apply(fs, from, to)
    }
  }
}

object rm {
  def apply(fs: FileSystem, path: String, recursive: Boolean): Boolean = {
    fs.delete(new Path(path), recursive)
  }
}

// object run extends App {
//   val config = new Configuration
//   val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
//   val runningCluster = cluster.build()
//   val fs = runningCluster.getFileSystem()
//   val from = "dst/could/be/any/file.txt"
//   val to = "dst/could/be/any/new/dir/parent/"
//   dfs.touch(fs, from, false)
//   println("========== LOGGING ====================")
//   val isMoved = dfs.mv(fs, from, to)
//   println(isMoved)
//   println("========== LOGGING ====================")
//   runningCluster.shutdown()
// }