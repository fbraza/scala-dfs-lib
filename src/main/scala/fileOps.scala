package dfs

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, PathFilter}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.ipc.RemoteException
import java.rmi.Remote

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration

/** Create a file to the specified path with default permissions. Create missing
  * parent directories found in the path. You can decide to (i) overwrite the
  * file, set (ii) the hadoop replication factor, (iii) the hadoop block size,
  * (iv) the writing buffer size. To create directories please prefer using
  * [[dfs.mkdir]]
  */
object touch {

  /** Create file at indicated path. If specified, can overwrite exisisting file
    *
    * @param fs
    *   Hadoop filesystem
    * @param path
    *   file path
    * @param overwrite
    *   if set to true overwrite existing file
    * @return
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean
  ): Boolean = {
    val filePath = new Path(path)
    if (cannotOverwrite(fs, overwrite, filePath)) {
      println(s"dfs.touch: $path : cannot be overwritten, set overwrite to true")
      false
    } else {
      fs.create(new Path(path), overwrite).close()
      true
    }
  }

  /** Create file at indicated path and with specified bufferSize. If specified,
    * can overwrite exisisting file.
    *
    * @param fs
    *   Hadoop filesystem
    * @param path
    *   file path
    * @param overwrite
    *   if set to true overwrite existing file
    * @param bufferSize
    *   size of the writing buffer, should be a multiple of 4096
    * @return
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean,
      bufferSize: Short
  ): Boolean = {
    val filePath = new Path(path)
    if (cannotOverwrite(fs, overwrite, filePath)) {
      println(s"dfs.touch: $path : cannot be overwritten, set overwrite to true")
      false
    } else {
      fs.create(filePath, bufferSize).close()
      true
    }
  }

  /** Create file at indicated path and with specified bufferSize, hadoop
    * replication and hadoop block size. If specified, can overwrite exisisting
    * file.
    *
    * @param fs
    *   Hadoop filesystem
    * @param path
    *   file path
    * @param overwrite
    *   if set to true overwrite existing file
    * @param bufferSize
    *   size of the writing buffer, should be a multiple of 4096
    * @param replicationFactor
    *   Hadoop replication factor on data nodes
    * @param blockSize
    *   Hadoop file block size on data nodes
    * @return
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean,
      bufferSize: Int,
      replicationFactor: Short,
      blockSize: Long
  ): Boolean = {
    val filePath = new Path(path)
    if (cannotOverwrite(fs, overwrite, filePath)) {
      println(s"dfs.touch: $path : cannot be overwritten, set overwrite to true")
      false
    } else {
      fs.create(filePath, overwrite, bufferSize, replicationFactor, blockSize)
        .close()
      true
    }
  }

  def cannotOverwrite(fs: FileSystem, overwrite: Boolean, path: Path): Boolean =
    !overwrite && dfs.exists(fs, path)
}

object mkdir {

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @return
    *   true if operation succeded false if not or path already exists
    */
  def apply(fs: FileSystem, path: String): Boolean = {
    val pathDir = new Path(path)
    if (dfs.exists(fs, pathDir)) {
      false
    } else {
      fs.mkdirs(pathDir)
    }
  }
}

object mv {

  /** Similar to the mv command in Bash. Use to rename or move files and folders
    *
    * @param fs
    * @param source
    * @param destination
    * @return true when process succeeds false otherwise
    */
  def apply(fs: FileSystem, source: String, destination: String): Boolean = {
    if (!dfs.exists(fs, source)) {
      println(s"dfs.mv: $source : No such file or directory")
      false
    }
    val srcPath = new Path(source)
    val dstPath = new Path(destination)
    val isSuccess = fs.rename(srcPath, dstPath)
    if (isSuccess) {
      isSuccess
    } else {
      println(s"dfs.mv: cannot move $source to $destination: No such file or directory")
      isSuccess
    }
  }

  object files {
    def apply(fs: FileSystem, source: String, destination: String): Unit = {
      fs.listStatus(new Path(source))
        .filter(status => status.isFile())
        .map(status => status.getPath().toString())
        .foreach(source => dfs.mv(fs, source, destination))
    }
  }

  object dirs {
    def apply(fs: FileSystem, source: String, destination: String): Unit = {
      fs.listStatus(new Path(source))
        .filter(status => status.isDirectory())
        .map(status => status.getPath().toString())
        .foreach(source => dfs.mv(fs, source, destination))
    }
  }
}
object cp {
// static boolean 	copy(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf)
}

object run extends App {
  val config = new Configuration
  val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
  val runningCluster = cluster.build()
  val fs = runningCluster.getFileSystem()
  val pathFile1 = "my/test/file/to_move.txt"
  val pathFile2 = "my/test/file/not_exist.txt"
  val pathDir1 = "my/test/directory/"
  dfs.touch(fs = fs, path = pathFile1, overwrite = false)
  dfs.mkdir(fs = fs, path = pathDir1)
  dfs.mv(fs = fs, source = pathFile2, destination = pathDir1)
  runningCluster.shutdown()
}

/** cp -> cp -> (toLocal: boolean ToRemote:boolean) rm -> delete, deleteOnExit
  */

// -rwxr-xr-x (represented in octal notation as 0755)
