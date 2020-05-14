name := "SparkEssentials"

version := "0.1"

organization := "com.github.rangaAmirapu.spark"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5"
)
