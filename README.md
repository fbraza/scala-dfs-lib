# DFS-Lib

![logo](./assets/dfs-lib.png)

## Functionality

**DFS-Lib** provides a simple Scala interface for the HDFS filesystem API.

## Setup

## Usage

Right now and because work is in progress DFS-Lib does not aim to be a complete replacement for the `org.apache.hadoop.fs`. That being said, DFS-Lib should provide all necessary functions to perform basic and advanced file operations.

### touch

It creates a file to the specified path with default permissions. Keep in mind, that any missing parent directory found in the path is also created (default behavior brought by the java API, it might change in the future). You can decide to (i) overwrite the file, set (ii) the hadoop replication factor, (iii) the hadoop block size, (iv) the writing buffer size. It returns `true` when operation is successful:

```scala
val fs = yourHadoopClusterInstance.getFileSystem()
val pathFile = "parent/directory/test_file01.txt"
dfs.touch(fs = fs, path = pathFile)
```

### mkdir

### mv

### mv.into

### mv.over

### cp

### rm

## For developers

## Author

## Acknowledgement

I would like to thank @lihaoyi for his fabulous scala libraries and notably the OS-Lib tool he developed. I got heavely inspired by the way code is written there.
