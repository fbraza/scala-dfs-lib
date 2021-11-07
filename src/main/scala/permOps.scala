package dfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.permission.FsPermission

/** Create a FsPermission object using unix string */
object Perm {
  /**
    *
    *
    * @param unixPermission
    * @return
    */
  def apply(unixPermission: String): FsPermission =
    FsPermission.valueOf(unixPermission)
}

/** Set owner ...
  */
object chown {}

/** Set permission
  */
object chmod {
  def apply(fs: FileSystem, path: String, perm: FsPermission): Unit = {
    val pathToSet = new Path(path)
    fs.setPermission(pathToSet, perm)
  }
}
