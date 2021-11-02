package dfs

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.conf.Configuration

/** Create a file to the specified path. It optionnaly accepts:
  *   - a string to set set permission using the unix format // TODO
  *   - a short integer to set the hadoop replication factor
  *   - a boolean to specify if file can be overwritten if already present
  *   - an interger to set the size of the writting buffer
  *   - a long integer to set the hadoop block size
  * By default it overwrites file if already present and recursively
  * create parent missing parent folder. To create directories please use
  * [[dfs.mkdir]]
  */
object touch {

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @return
    *   a FSDataOutputStream
    */
  def apply(
      fs: FileSystem,
      path: String,
  ): FSDataOutputStream =
    fs.create(new Path(path))

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @param overwrite
    *   if set to true it overwrites existing file else throws an error
    * @return
    *   a FSDataOutputStream at indicated path
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean
  ): FSDataOutputStream =
    fs.create(new Path(path), overwrite)

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @param replicationFactor
    *   determines how much time the file block should be replicated
    * @return
    *   a FSDataOutputStream at indicated path
    */
  def apply(
      fs: FileSystem,
      path: String,
      replicationFactor: Short
  ): FSDataOutputStream =
    fs.create(new Path(path), replicationFactor)

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @param overwrite
    *   if set to true it overwrites existing file else throws an error
    * @param bufferSize
    *   determines how much data is buffered during read and write operations
    *   (multiple of 4096 for intel x86)
    * @return
    *   a FSDataOutputStream at indicated path
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean,
      bufferSize: Int
  ): FSDataOutputStream =
    fs.create(new Path(path), overwrite, bufferSize)

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @param overwrite
    *   if set to true it overwrites existing file else throws an error
    * @param bufferSize
    *   determines how much data is buffered during read and write operations
    * @param replicationFactor
    *   hdfs replication factor of your file
    * @param blockSize
    *   hdfs file block size, 134217728by default
    * @return
    *   a FSDataOutputStream at indicated path
    */
  def apply(
      fs: FileSystem,
      path: String,
      overwrite: Boolean,
      bufferSize: Int,
      replicationFactor: Short,
      blockSize: Long
  ): FSDataOutputStream =
    fs.create(
      new Path(path),
      overwrite,
      bufferSize,
      replicationFactor,
      blockSize
    )
}

/** Create a folder to the specified path. It optionnaly accepts:
  *   - a string to set set permission using the unix format
  *   - a short integer to set permission in the octal format
  *   - a boolean to specify if file can be overwritten if already present
  */
object mkdir {

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @return
    *   true if process has been successful false otherwise
    */
  def apply(fs: FileSystem, path: String): Boolean =
    fs.mkdirs(new Path(path))

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @param unix
    *   a Unix symbolic permission string
    * @return
    *   true if process has been successful false otherwise
    */
  def apply(fs: FileSystem, path: String, unix: String): Boolean =
    fs.mkdirs(new Path(path), dfs.Perm(unix))

  /** @param fs
    *   an instance of the java hadoop FileSystem
    * @param path
    *   absolute path of the file to be created
    * @param octal
    *   a Unix symbolic permission string
    * @return
    *   true if process has been successful false otherwise
    */
  def apply(fs: FileSystem, path: String, octal: Short): Boolean =
    fs.mkdirs(new Path(path), dfs.Perm(octal))
}

object run extends App {
  val config = new Configuration
  val cluster = new MiniDFSCluster.Builder(config).numDataNodes(1)
  val runningCluster = cluster.build()
  val fs = runningCluster.getFileSystem()
  val pathFile = "parent/directory/dummy.txt"
  touch(fs, pathFile, false)
  val fileStat = fs.getFileStatus(new Path(pathFile))
  println("-------- FILE STATS ----------")
  println("BLOCKSIZE: " + fileStat.getBlockSize())
  println("BLOCKSIZE: " + fileStat.getOwner())
  println("BLOCKSIZE: " + fileStat.getPermission())
  println("BLOCKSIZE: " + fileStat.getReplication())
  runningCluster.shutdown()
}

/** touch mkdir >> for concatenating files move and rename use mv
  */

  // -rwxr-xr-x (represented in octal notation as 0755)