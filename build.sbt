ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "RevProject2"
  )

libraryDependencies += "org.apache.zeppelin" % "zeppelin-spark" % "0.6.0"
libraryDependencies += "org.apache.zeppelin" % "zeppelin-interpreter" % "0.10.0"
libraryDependencies += "org.apache.zeppelin" % "zeppelin-server" % "0.10.0"
libraryDependencies += "org.apache.zeppelin" % "zeppelin-client" % "0.10.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0"
libraryDependencies += "com.outr" %% "hasher" % "1.2.2"