# DFS-Lib

![logo](./assets/dfs-lib.png)

Simple Scala interface for HDFS filesystem operations.

---

## Setup

Add to your `build.sbt`:

```scala
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.8.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.8.1"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
```

---

## Usage

Import the library and initialize your filesystem:

```scala
import dfs._
import org.apache.hadoop.fs.FileSystem

implicit val fs: FileSystem = yourHadoopClusterInstance.getFileSystem()
```

### File operations

#### Create files

Create a file with automatic parent directory creation:

```scala
val created = touch("path/to/file.txt")
```

Create with custom parameters:

```scala
val created = touch(
  path = "path/to/file.txt",
  overwrite = true,
  bufferSize = 8192,
  replicationFactor = 3,
  blockSize = 268435456
)
```

#### Create directories

Create a directory and all parent directories:

```scala
val created = mkdir("path/to/directory")
```

#### Move and rename

Basic move operation:

```scala
val moved = mv("source/path", "destination/path")
```

Move into a directory (creates parents if needed):

```scala
val moved = mv.into("source/path", "destination/directory")
```

Move with overwrite:

```scala
val moved = mv.over("source/path", "destination/path")
```

#### Copy operations

Copy a single file:

```scala
cp(fs, "source/file.txt", "destination/file.txt")
```

Copy directories recursively:

```scala
cp.recursive(fs, "source/directory", "destination/directory")
```

#### Remove operations

Remove a file:

```scala
val removed = rm("path/to/file.txt")
```

Remove directories recursively:

```scala
val removed = rm.r("path/to/directory")
```

### File inspection

#### Check existence

```scala
val fileExists = exists("path/to/file.txt")
val isDir = isDirectory("path/to/directory")
val isFile = isFile("path/to/file.txt")
```

#### Get file information

Get file size:

```scala
val fileSize = size("path/to/file.txt")
```

Get comprehensive file metadata:

```scala
val metadata = stat("path/to/file.txt")
println(s"Size: ${metadata.size} bytes")
println(s"Owner: ${metadata.owner}")
println(s"Permissions: ${metadata.permissions}")
```

#### List directory contents

List files in directory:

```scala
val files = ls(fs, "path/to/directory")
```

List with detailed information:

```scala
val details = ls.details(fs, "path/to/directory")
println(details)
```

### File content operations

#### Read file contents

Read entire file:

```scala
val content = cat(fs, "path/to/file.txt")
```

Read with line numbers:

```scala
val content = cat.numbered(fs, "path/to/file.txt")
```

Read first N lines:

```scala
val head = cat.head(fs, "path/to/file.txt", lines = 20)
```

Read last N lines:

```scala
val tail = cat.tail(fs, "path/to/file.txt", lines = 10)
```

### Permission operations

#### Change ownership

Change file owner:

```scala
chown("path/to/file.txt", "newowner")
```

Change owner and group:

```scala
chown("path/to/file.txt", "newowner", "newgroup")
```

Recursive ownership change:

```scala
chown.r("path/to/directory", "newowner", "newgroup")
```

#### Change permissions

Set file permissions using Unix-style permissions:

```scala
val permissions = Perm("755")
chmod("path/to/file.txt", permissions)
```

---

## Error handling

All operations return `Boolean` for success/failure or throw exceptions for critical errors. Failed operations are logged with detailed error messages.

---

## For developers

Run tests:

```bash
sbt test
```

---

## Acknowledgement

Special thanks to @lihaoyi for his Scala libraries, particularly OS-Lib, which heavily inspired this library's design patterns.
