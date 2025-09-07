package dfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.fs.permission.FsPermission
import java.io.{FileNotFoundException, IOException}

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

/** Change file ownership
  */
object chown {
  /** Change file owner
    * @param fs FileSystem instance
    * @param path Path to the file/directory
    * @param owner New owner username
    * @throws FileNotFoundException if the path does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def apply(fs: FileSystem, path: String, owner: String): Unit = {
    val pathObj = new Path(path)
    try {
      if (!fs.exists(pathObj)) {
        throw new FileNotFoundException(s"Path does not exist: $path")
      }
      fs.setOwner(pathObj, owner, null)
    } catch {
      case e: FileNotFoundException =>
        throw e
      case e: IOException =>
        throw new IOException(s"Failed to change owner for $path: ${e.getMessage}", e)
    }
  }
  
  /** Change file owner and group
    * @param fs FileSystem instance
    * @param path Path to the file/directory
    * @param owner New owner username
    * @param group New group name
    * @throws FileNotFoundException if the path does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def apply(fs: FileSystem, path: String, owner: String, group: String): Unit = {
    val pathObj = new Path(path)
    try {
      if (!fs.exists(pathObj)) {
        throw new FileNotFoundException(s"Path does not exist: $path")
      }
      fs.setOwner(pathObj, owner, group)
    } catch {
      case e: FileNotFoundException =>
        throw e
      case e: IOException =>
        throw new IOException(s"Failed to change owner for $path: ${e.getMessage}", e)
    }
  }

  /** Recursively change ownership for directories
    */
  object recursive {
    /** Recursively change owner for directory and all contents
      * @param fs FileSystem instance
      * @param path Path to the directory
      * @param owner New owner username
      * @throws FileNotFoundException if the path does not exist
      * @throws IOException if there is an I/O error or permission denied
      */
    def apply(fs: FileSystem, path: String, owner: String): Unit = {
      apply(fs, path, owner, null)
    }
    
    /** Recursively change owner and group for directory and all contents
      * @param fs FileSystem instance
      * @param path Path to the directory
      * @param owner New owner username
      * @param group New group name (optional)
      * @throws FileNotFoundException if the path does not exist
      * @throws IOException if there is an I/O error or permission denied
      */
    def apply(fs: FileSystem, path: String, owner: String, group: String): Unit = {
      val pathObj = new Path(path)
      
      try {
        if (!fs.exists(pathObj)) {
          throw new FileNotFoundException(s"Path does not exist: $path")
        }
        
        def changeOwnerRecursive(current: Path): Unit = {
          val status = fs.getFileStatus(current)
          fs.setOwner(current, owner, group)
          
          if (status.isDirectory) {
            val files = fs.listStatus(current)
            files.foreach { fileStatus =>
              changeOwnerRecursive(fileStatus.getPath)
            }
          }
        }
        
        changeOwnerRecursive(pathObj)
      } catch {
        case e: FileNotFoundException =>
          throw e
        case e: IOException =>
          throw new IOException(s"Failed to recursively change owner for $path: ${e.getMessage}", e)
      }
    }
  }
}

/** Set permission
  */
object chmod {
  def apply(fs: FileSystem, path: String, perm: FsPermission): Unit = {
    val pathToSet = new Path(path)
    fs.setPermission(pathToSet, perm)
  }
}
