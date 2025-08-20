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
import java.io.FileNotFoundException

/** Check if a file or directory exists */
object exists {
  def apply(path: String)(implicit fs: FileSystem): Boolean =
    fs.exists(new Path(path))

  def apply(path: Path)(implicit fs: FileSystem): Boolean =
    fs.exists(path)
}

/** Get file or directory size in bytes */
object size {
  def apply(path: String)(implicit fs: FileSystem): Long = {
    if (!exists(path)) {
      throw new java.io.FileNotFoundException(s"File not found: $path")
    }
    fs.getFileStatus(new Path(path)).getLen
  }
}

/** Get replication factor for a file */
object replication {
  def apply(path: String)(implicit fs: FileSystem): Short = {
    if (!exists(path)) {
      throw new java.io.FileNotFoundException(s"File not found: $path")
    }
    fs.getFileStatus(new Path(path)).getReplication
  }
}

/** Get block size for a file */
object blockSize {
  def apply(path: String)(implicit fs: FileSystem): Long = {
    if (!exists(path)) {
      throw new java.io.FileNotFoundException(s"File not found: $path")
    }
    fs.getFileStatus(new Path(path)).getBlockSize
  }
}

/** Get normalized path */
object getPath {
  def apply(path: String)(implicit fs: FileSystem): String = {
    if (!exists(path)) {
      throw new java.io.FileNotFoundException(s"File not found: $path")
    }
    new Path(path).toString
  }
}

/** Check if a path is a file */
object isFile {
  def apply(fs: FileSystem, path: String): Boolean =
    fs.getFileStatus(new Path(path)).isFile()
}

/** Check if a path is a directory */
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

/** Comprehensive file metadata case class and stat object */
object stat {
  case class FileMetadata(
    path: String,
    size: Long,
    isFile: Boolean,
    isDirectory: Boolean,
    modificationTime: Long,
    accessTime: Long,
    owner: String,
    group: String,
    permissions: String,
    replication: Short,
    blockSize: Long
  )

  def apply(path: String)(implicit fs: FileSystem): FileMetadata = {
    if (!exists(path)) {
      throw new java.io.FileNotFoundException(s"File not found: $path")
    }
    val status = fs.getFileStatus(new Path(path))
    FileMetadata(
      path = status.getPath.toString,
      size = status.getLen,
      isFile = status.isFile,
      isDirectory = status.isDirectory,
      modificationTime = status.getModificationTime,
      accessTime = status.getAccessTime,
      owner = status.getOwner,
      group = status.getGroup,
      permissions = status.getPermission.toString,
      replication = status.getReplication,
      blockSize = status.getBlockSize
    )
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
