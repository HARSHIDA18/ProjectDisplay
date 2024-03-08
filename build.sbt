ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "FinalProjectShow"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.0.0", // Updated to the latest stable version
  "com.typesafe.akka" %% "akka-http" % "10.2.6", // Adjusted to a known compatible version
  "com.typesafe.akka" %% "akka-stream" % "2.6.17", // Adjusted to a known compatible version
  "ch.qos.logback" % "logback-classic" % "1.2.6",
  "org.playframework" %% "play-json" % "3.0.2",
  "mysql" % "mysql-connector-java" % "8.0.28"
)