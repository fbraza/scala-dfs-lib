package dfs

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration

/** Create a file to the specified path with default permissions. Create missing
  * parent directories found in the path. You can decide to (i) overwrite the
  * file, set (ii) the hadoop replication factor, (iii) the hadoop block size,
  * (iv) the writing buffer size. To create directories please prefer using
  * [[dfs.mkdir]]
  */
object touch {

  /**
    * Create file at indicated path. If specified, can overwrite exisisting file
    *
    * @param fs Hadoop filesystem
    * @param path file path
    * @param overwrite if set to true overwrite existing file
    * @return true if operation succeeds false otherwise
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean
  ): Boolean = {
    val filePath = new Path(path)
    if (cannotOverwrite(fs, overwrite, filePath)) false
    else {
      fs.create(new Path(path), overwrite).close()
      true
    }
  }

  /**
    * Create file at indicated path and with specified bufferSize.
    * If specified, can overwrite exisisting file.
    *
    * @param fs Hadoop filesystem
    * @param path file path
    * @param overwrite if set to true overwrite existing file
    * @param bufferSize size of the writing buffer, should be a multiple of 4096
    * @return true if operation succeeds false otherwise
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean,
      bufferSize: Short
  ): Boolean = {
    val filePath = new Path(path)
    if (cannotOverwrite(fs, overwrite, filePath)) false
    else {
      fs.create(filePath, bufferSize).close()
      true
    }
  }

  /**
    * Create file at indicated path and with specified bufferSize, hadoop replication
    * and hadoop block size. If specified, can overwrite exisisting file.
    *
    * @param fs Hadoop filesystem
    * @param path file path
    * @param overwrite if set to true overwrite existing file
    * @param bufferSize size of the writing buffer, should be a multiple of 4096
    * @param replicationFactor Hadoop replication factor on data nodes
    * @param blockSize Hadoop file block size on data nodes
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
    if (cannotOverwrite(fs, overwrite, filePath)) false
    else {
      fs.create(filePath, overwrite, bufferSize, replicationFactor, blockSize).close()
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
    *   true if operation succeded
    */
  def apply(fs: FileSystem, path: String): Boolean = {
    val pathDir = new Path(path)
    fs.mkdirs(pathDir)
  }
}

object mv {
  // use the rename function for hadoop API
}
object rm {
  // use the delete function for hadoop API
}
object cp {
// static boolean 	copy(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf)
}

// object run extends App {
//   val config = new Configuration
//   val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
//   val runningCluster = cluster.build()
//   val fs = runningCluster.getFileSystem()
//   val pathDir1 = "my/test/directoy_permission_unix/"
//   val fileStat = fs.getFileStatus(new Path(pathDir1))
//   println("-------- FOLDER STATS ----------")
//   println(fs.getFileStatus(new Path(pathDir1)).getPermission())
//   println("-------- FOLDER STATS ----------")
//   runningCluster.shutdown()
// }

/** mv -> (moveOnly, renameOnly) cp -> cp -> (toLocal: boolean ToRemote:
  * boolean) rm -> delete, deleteOnExit
  */

// -rwxr-xr-x (represented in octal notation as 0755)
