package dfs

import scala.concurrent.ExecutionContext.Implicits.global
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
  * files/dirs operations return false otherwise true.
  */
object touch {
  val logger = Logger("dfs.touch.scala")

  /** Create file and all its parent directories if not existing with default permissions.
    * It overwrites exisisting file when specified. Writing buffer size, replication factor
    * and block size can be specified.
    *
    * @param path a string of the file absolute path
    * @param overwrite if set to true overwrite existing file
    * @param bufferSize size of the writing buffer, 4096 by default
    * @param replicationFactor Hadoop replication factor on data nodes, 1 by default
    * @param blockSize Hadoop file block size on data nodes, 134217728 bytes by default
    * @param fs an instance of the Hadoop file system
    * @return true if operation succeeds false otherwise
    */
  def apply(
      path: String,
      overwrite: Boolean = false,
      bufferSize: Int = 4096,
      replicationFactor: Short = 1.toShort,
      blockSize: Long = 134217728
  )(implicit fs: FileSystem): Boolean = {
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
    * @param path absolute path of the file to be created
    * @param fs an instance of the Hadoop file system
    * @return true if operation succeeded false otherwise
    */
  def apply(path: String)(implicit fs: FileSystem): Boolean = {
    val pathDir = new Path(path)
    var isCreated = true
    if (dfs.exists(pathDir)) {
      logger.error(s"cannot create directory at path : $path : already exists")
      !isCreated
    } else {
        try {
          fs.mkdirs(pathDir)
          logger.info(s"directory created at path : $path")
          isCreated
        } catch {
          case pde: ParentNotDirectoryException =>
            logger.error(s"in the path : $path : a parent must not be a file")
            !isCreated
        }
    }
  }
}

/** Rename or move files or folders to the specified path. The [[dfs.mv]] function
  * provides basic behavior. If you want to move a file or folder into a specific
  * path (even if parents do not exist) use [[dfs.mv.into]]. To overwrite the
  * destination use [[dfs.mv.over]]
  *
  * Code has been implemented to abstract away the most common errors raised by
  * the Hadoop FileSystem Java API. Errors are catched but always logged. Failed
  * files/dirs operations return false otherwise true.
  */
object mv {
  val logger = Logger("dfs.mv.scala")

  /** Use to move a file or a folder to the specified path.
    *
    * @param from source path
    * @param to destination path
    * @param fs an instance of the Hadoop file system
    * @return true when process succeeds false otherwise
    */
  def apply(from: String, to: String)(implicit fs: FileSystem): Boolean = {
    val isMoved = true
    if (to.startsWith(from)) {
      logger.error(s"cannot move : $from : to : $to : destination cannot be a descendant of the source : $from ")
      !isMoved
    } else if(!dfs.exists(from)) {
        logger.error(s"cannot move : $from : to : $to : because it is not found")
        !isMoved
    } else if(dfs.exists(to) && dfs.isFile(to)) {
        logger.error(s"cannot move : $from : to : $to : because an existing file is found the end of the destination path")
        !isMoved
    } else if(!doAllParentDirExist(to)) {
        logger.error(s"cannot move : $from : to : $to : because parent(s) or descendant(s) is (are) missing")
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
          logger.error(s"cannot move : $to : the parents must not be a file")
          !isMoved
      }
    }
  }

  /** Move a file or directory inside a specified directory.
    * Create missing parent directory if any
    *
    * @param from source path
    * @param to destination path
    * @param fs an instance of the Hadoop file system
    * @return true when process succeeds false otherwise
    */
  object into {
    val loggerInto = Logger("dfs.mv.into.scala")

    /** Move a file or folder to the specified destination.
      * If destination does not exist it will create it.
      *
      * @param from source path
      * @param to destination path
      * @param fs an instance of the Hadoop file system
      * @return true when process succeeds false otherwise
      */
    def apply(from: String, to: String)(implicit fs: FileSystem): Boolean = {
      val isMoved = true
      if (!doAllParentDirExist(to)) {
        mkdir(to) && mv(from, to)
      } else if (!dfs.isDirectory(to)) {
        loggerInto.error(s"the destination : $to : must be a directory")
        !isMoved
      } else {
        mv.apply(from, to)
      }
    }
  }

  object over {

    /** Move a file or a folder and overwrite its destination
      *
      *
      * @param from source path
      * @param to destination path
      * @param fs an instance of the Hadoop file system
      * @return true when process succeeds false otherwise
      */
    def apply(from: String, to: String)(implicit fs: FileSystem): Boolean = false
  }
}

object rm {
  val logger = Logger("dfs.rm.scala")

  /** Remove files only
    *
    *
    * @param path file to delete
    * @param fs an instance of the Hadoop file system
    * @return
    */
  def apply(path: String)(implicit fs: FileSystem): Boolean = {
    val isDeleted = true
    if (!dfs.exists(path)) {
      logger.error(s"cannot remove ${path}: Does not exists")
      !isDeleted
    } else if (dfs.isDirectory(path)) {
      logger.error(s"cannot remove ${path}: It is a directory")
      !isDeleted
    } else {
      val deleted = fs.delete(new Path(path))
      logger.info(s"${path}: has been removed")
      deleted
    }
  }

  object r {
    val logger = Logger("dfs.rm.r.scala")

    /** Remove directories recursively
      *
      *
      * @param path directory to delete
      * @param fs an instance of the Hadoop file system
      * @return true if folder is deleted false otherwise
      */
    def apply(path: String)(implicit fs: FileSystem): Boolean = {
      val isDeleted = true
      if (!dfs.exists(path)){
        logger.error(s"cannot remove ${path}: Does not exists")
        !isDeleted
      } else if (isRootDir(path)) {
        logger.error(s"cannot remove ${path}: It is a root directory")
        !isDeleted
      } else if (dfs.isFile(path)) {
        logger.error(s"cannot remove recursively ${path}: It is a file")
        !isDeleted
      } else {
        val deleted = fs.delete(new Path(path), true)
        logger.info(s"${path}: has been removed")
        isDeleted && deleted
      }
    }
  }
}

object run extends App {
  val config = new Configuration
  val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
  val runningCluster = cluster.build()
  implicit val fs = runningCluster.getFileSystem()
  val target = "/usr/my/directory"
  println("========== LOGGING ====================")
  mkdir(path = target)
  println("========== LOGGING ====================")
  runningCluster.shutdown()
}