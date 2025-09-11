package dfs

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, PathFilter}
import org.apache.hadoop.fs.permission.FsPermission
import java.io.{FileNotFoundException, IOException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import com.typesafe.scalalogging.Logger

object cp {
  val logger = Logger("dfs.cp.scala")

  /** Copy file from source to destination
    * @param fs FileSystem instance
    * @param src Source file path
    * @param dst Destination file path
    * @throws FileNotFoundException if the source file does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def apply(fs: FileSystem, src: String, dst: String): Unit = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)
    
    if (!fs.exists(srcPath)) {
      throw new FileNotFoundException(s"Source file not found: $src")
    }
    
    if (fs.isDirectory(srcPath)) {
      throw new IOException(s"Source is a directory. Use cp.recursive for directories: $src")
    }
    
    try {
      FileUtil.copy(fs, srcPath, fs, dstPath, false, fs.getConf)
      logger.info(s"Copied file from $src to $dst")
    } catch {
      case e: IOException =>
        throw new IOException(s"Failed to copy file from $src to $dst: ${e.getMessage}", e)
    }
  }
  
  object recursive {
    val loggerRecursive = Logger("dfs.cp.recursive.scala")

    /** Recursively copy directory and all contents
      * @param fs FileSystem instance
      * @param src Source directory path
      * @param dst Destination directory path
      * @throws FileNotFoundException if the source directory does not exist
      * @throws IOException if there is an I/O error or permission denied
      */
    def apply(fs: FileSystem, src: String, dst: String): Unit = {
      val srcPath = new Path(src)
      val dstPath = new Path(dst)
      
      if (!fs.exists(srcPath)) {
        throw new FileNotFoundException(s"Source directory not found: $src")
      }
      
      if (!fs.isDirectory(srcPath)) {
        throw new IOException(s"Source is not a directory: $src")
      }
      
      try {
        FileUtil.copy(fs, srcPath, fs, dstPath, false, true, fs.getConf)
        loggerRecursive.info(s"Recursively copied directory from $src to $dst")
      } catch {
        case e: IOException =>
          throw new IOException(s"Failed to recursively copy directory from $src to $dst: ${e.getMessage}", e)
      }
    }
  }
}

object ls {
  val logger = Logger("dfs.ls.scala")

  /** List directory contents
    * @param fs FileSystem instance
    * @param path Directory path to list
    * @return Array of file statuses
    * @throws FileNotFoundException if the directory does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def apply(fs: FileSystem, path: String): Array[FileStatus] = {
    val pathObj = new Path(path)
    
    if (!fs.exists(pathObj)) {
      throw new FileNotFoundException(s"Directory not found: $path")
    }
    
    if (!fs.isDirectory(pathObj)) {
      throw new IOException(s"Path is not a directory: $path")
    }
    
    try {
      val files = fs.listStatus(pathObj)
      logger.info(s"Listed directory contents for $path (${files.length} items)")
      files
    } catch {
      case e: IOException =>
        throw new IOException(s"Failed to list directory contents for $path: ${e.getMessage}", e)
    }
  }
  
  /** List directory contents with filtering
    * @param fs FileSystem instance
    * @param path Directory path to list
    * @param filter Optional file filter
    * @return Array of filtered file statuses
    * @throws FileNotFoundException if the directory does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def apply(fs: FileSystem, path: String, filter: PathFilter): Array[FileStatus] = {
    val pathObj = new Path(path)
    
    if (!fs.exists(pathObj)) {
      throw new FileNotFoundException(s"Directory not found: $path")
    }
    
    if (!fs.isDirectory(pathObj)) {
      throw new IOException(s"Path is not a directory: $path")
    }
    
    try {
      val files = fs.listStatus(pathObj, filter)
      logger.info(s"Listed filtered directory contents for $path (${files.length} items)")
      files
    } catch {
      case e: IOException =>
        throw new IOException(s"Failed to list filtered directory contents for $path: ${e.getMessage}", e)
    }
  }
  
  object details {
    val loggerDetails = Logger("dfs.ls.details.scala")

    /** List directory contents with detailed information
      * @param fs FileSystem instance
      * @param path Directory path to list
      * @return Formatted string with file details
      * @throws FileNotFoundException if the directory does not exist
      * @throws IOException if there is an I/O error or permission denied
      */
    def apply(fs: FileSystem, path: String): String = {
      val files = ls(fs, path)
      files.map { status =>
        val permissions = status.getPermission.toString
        val owner = status.getOwner
        val group = status.getGroup
        val size = status.getLen
        val date = new java.util.Date(status.getModificationTime)
        val name = status.getPath.getName
        
        f"$permissions%-10s $owner%-8s $group%-8s $size%8d $date%20s $name"
      }.mkString("\n")
    }
  }
}

object cat {
  val logger = Logger("dfs.cat.scala")

  /** Display file contents as text
    * @param fs FileSystem instance
    * @param path File path to display
    * @return File contents as string
    * @throws FileNotFoundException if the file does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def apply(fs: FileSystem, path: String): String = {
    val pathObj = new Path(path)
    
    if (!fs.exists(pathObj)) {
      throw new FileNotFoundException(s"File not found: $path")
    }
    
    if (!fs.isFile(pathObj)) {
      throw new IOException(s"Path is not a file: $path")
    }
    
    val inputStream = fs.open(pathObj)
    try {
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      logger.info(s"Read file contents from $path (${content.length} characters)")
      content
    } finally {
      inputStream.close()
    }
  }
  
  /** Display file contents with line numbers
    * @param fs FileSystem instance
    * @param path File path to display
    * @return File contents with line numbers
    * @throws FileNotFoundException if the file does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def numbered(fs: FileSystem, path: String): String = {
    val content = apply(fs, path)
    content.split("\n").zipWithIndex.map { case (line, idx) =>
      f"${idx + 1}%4d: $line"
    }.mkString("\n")
  }
  
  /** Display first N lines of file
    * @param fs FileSystem instance
    * @param path File path to display
    * @param lines Number of lines to display
    * @return First N lines as string
    * @throws FileNotFoundException if the file does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def head(fs: FileSystem, path: String, lines: Int = 10): String = {
    val content = apply(fs, path)
    content.split("\n").take(lines).mkString("\n")
  }
  
  /** Display last N lines of file
    * @param fs FileSystem instance
    * @param path File path to display
    * @param lines Number of lines to display
    * @return Last N lines as string
    * @throws FileNotFoundException if the file does not exist
    * @throws IOException if there is an I/O error or permission denied
    */
  def tail(fs: FileSystem, path: String, lines: Int = 10): String = {
    val content = apply(fs, path)
    content.split("\n").takeRight(lines).mkString("\n")
  }
}