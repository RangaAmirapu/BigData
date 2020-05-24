name := "SparkStreaming"

version := "0.1"

organization := "com.github.rangaAmirapu.SparkStreaming"

scalaVersion := "2.12.7"

val sparkVersion = "2.4.5"
val twitterLibVersion = "4.0.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.twitter4j" % "twitter4j-core" % twitterLibVersion,
  "org.twitter4j" % "twitter4j-stream" % twitterLibVersion
)