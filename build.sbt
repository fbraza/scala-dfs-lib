name := "DFS-Lib"

version := "1.0"

// Cross-compilation support
crossScalaVersions := Seq("2.12.20", "2.13.15", "3.3.1")
scalaVersion := crossScalaVersions.value.head

// Scala 2/3 compatibility
libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.apache.hadoop" % "hadoop-hdfs" % "2.8.1" % "compile,test" classifier "" classifier "tests",
  "org.apache.hadoop" % "hadoop-common" % "2.8.1" % "compile,test" classifier "" classifier "tests",
  "org.apache.hadoop" % "hadoop-minicluster" % "2.8.1" % "compile,test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
)

// Add Scala 3 specific compiler options if needed
scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) =>
      Seq("-Xsource:3", "-Ywarn-unused:imports")
    case Some((3, _)) =>
      Seq("-source:3.0-migration", "-explain")
    case _ =>
      Seq.empty
  }
}

// Publishing configuration
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

organization := "com.fbraza" // Replace with your organization
homepage := Some(url("https://github.com/fbraza/scala-dfs-lib"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/fbraza/scala-dfs-lib"),
    "scm:git@github.com:fbraza/scala-dfs-lib.git"
  )
)
developers := List(
  Developer(
    id = "fbraza",
    name = "Faouzi Braza",
    email = "faouzi.brazza@gmail.com",
    url = url("https://github.com/fbraza")
  )
)
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
