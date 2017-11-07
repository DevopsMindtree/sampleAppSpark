name := "comAppSpark"

version := "0.1"

scalaVersion := "2.11.6"

name := "comAppSpark"

version := "0.1"

scalaVersion := "2.11.6"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  //"com.couchbase.client" %% "spark-connector_2.11" % sparkVersion

)

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.2" % "test"
//libraryDependencies += "com.couchbase.client" % "spark-connector_2.11" % "1.0.0"
