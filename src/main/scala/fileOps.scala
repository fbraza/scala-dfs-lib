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
  val logger = Logger("dfs.mkdir.scala")

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

  /** Use to move a file or a folder.
    *
    * @param fs
    *   an instance of the Hadoop file system
    * @param from
    * @param to
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
    } else if(dfs.exists(fs, to)) {
        logger.error(s"cannot move : $from : to : $to : because destination already exists")
        !isMoved
    } else {
      val fromPath = new Path(from)
      val toPath = new Path(to)
      try {
        fs.rename(fromPath, toPath)
        logger.info(s"source : $from : moved to path : $to")
        isMoved
      } catch {
        case pde: ParentNotDirectoryException =>
          logger.error(s"the parent path : $to : must not be a file")
          !isMoved
      }
    }
  }

  object into {
    def apply(fs: FileSystem, from: String, to: String): Boolean = {
      true
    }

  }

  object over {
    def apply(fs: FileSystem, from: String, to: String): Boolean = {
      dfs.rm(fs, to, true)
      mv.apply(fs, from, to)
    }
  }
}
object cp {
// static boolean 	copy(FileSystem fromFS, FileStatus fromStatus, FileSystem toFS, Path to, boolean deleteSource, boolean overwrite, Configuration conf)
}

object rm {
  def apply(fs: FileSystem, path: String, recursive: Boolean): Boolean = {
    fs.delete(new Path(path), recursive)
  }
}

object run extends App {
  val config = new Configuration
  val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
  val runningCluster = cluster.build()
  val fs = runningCluster.getFileSystem()
  val from = "source/dir/myDir/faouzi.txt"
  val to = "source/dir/otherDir/julie.txt"
  println("========== LOGGING ====================")
  val isMoved = dfs.mv(fs, from, to)
  println(isMoved)
  // DIR* FSDirectory.unprotectedRenameTo: Rename destination /user/fbraza/source/dir/myDir/faouzi/julie/myDir
  // is a directory or file under source /user/fbraza/source/dir/myDir
  println("========== LOGGING ====================")
  runningCluster.shutdown()
}

/** cp -> cp -> (toLocal: boolean ToRemote:boolean) rm -> delete, deleteOnExit
  */

// -rwxr-xr-x (represented in octal notation as 0755)
