package dfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.permission.FsPermission

/** Object to Define Permission: use the function to return FsPermission from
  * unix string use the function to return FsPermission from octal
  * representation Object factory to set Owner Object factory to set Permissions
  */

object Perm {
  def apply(unixPermission: String): FsPermission =
    FsPermission.valueOf(unixPermission)

  def apply(octalPermission: Short): FsPermission =
    new FsPermission(octalPermission)
}

object chown {}
object chmod {
  def apply(fs: FileSystem, path: Path, perm: FsPermission): Unit =
    fs.setPermission(path, perm)
}
