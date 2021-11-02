name := "DSTI-JSON-LOG-REPORT"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
"org.scalactic" %% "scalactic" % "3.2.2",
"org.scalatest" %% "scalatest" % "3.2.2" % "test",
"org.apache.spark" %% "spark-sql" % "3.0.1",
"org.apache.hadoop" % "hadoop-hdfs" % "2.8.1" % "compile,test" classifier "" classifier "tests",
"org.apache.hadoop" % "hadoop-common" % "2.8.1" % "compile,test" classifier "" classifier "tests",
"org.apache.hadoop" % "hadoop-minicluster" % "2.8.1" % "compile,test"
)
