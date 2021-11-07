/** All operations related to the FileStatus
  * Build a stat functions that mimick bash stat command
  * size
  * replicaton
  * block
  * getPath
  */
package dfs

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FSDataOutputStream

object exists {
  def apply(fs: FileSystem, path: String): Boolean =
    fs.exists(new Path(path))

  def apply(fs: FileSystem, path: Path): Boolean =
    fs.exists(path)
}

object isFile {
  def apply(fs: FileSystem, path: String): Boolean =
    fs.getFileStatus(new Path(path)).isFile()
}

object isDirectory {
  def apply(fs: FileSystem, path: String): Boolean =
    fs.getFileStatus(new Path(path)).isDirectory()
}
