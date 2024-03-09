ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "FinalProjectShow"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "3.0.0",
  "com.typesafe.akka" %% "akka-http" % "10.2.6",
  "com.typesafe.akka" %% "akka-stream" % "2.6.17",
  "ch.qos.logback" % "logback-classic" % "1.2.6",
  "com.typesafe.play" %% "play-json" % "2.10.0",
  "mysql" % "mysql-connector-java" % "8.0.28",
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0"
)