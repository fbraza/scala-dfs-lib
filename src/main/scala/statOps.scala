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
  def apply(path: String)(implicit fs: FileSystem): Boolean =
    fs.exists(new Path(path))

  def apply(path: Path)(implicit fs: FileSystem): Boolean =
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


/** Check if all parent directories exist for a given path */
object doAllParentDirExist {
  def apply(path: String)(implicit fs: FileSystem): Boolean = {
    val pathObj = new Path(path)
    val parent = pathObj.getParent()
    if (parent == null) true // Root path, no parent
    else fs.exists(parent)
  }
}

/** Check if a path is the root directory or a system directory that should not be deleted */
object isRootDir {
  def apply(path: String): Boolean = {
    val pathObj = new Path(path)
    val pathStr = pathObj.toString()
    // Check if it's actually the root directory "/"
    pathObj.getParent() == null ||
    // Or if it's a system directory like /usr, /bin, /etc, etc.
    pathStr.startsWith("/usr") ||
    pathStr.startsWith("/bin") ||
    pathStr.startsWith("/etc") ||
    pathStr.startsWith("/var") ||
    pathStr.startsWith("/tmp") ||
    pathStr.startsWith("/lib") ||
    pathStr.startsWith("/sbin")
  }
}
