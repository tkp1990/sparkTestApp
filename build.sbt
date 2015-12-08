name := "SparkEsTemp"

version := "1.0"

scalaVersion := "2.10.4"

crossScalaVersions  := Seq("2.11.7", "2.10.4")

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.1"

libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "2.2.0-beta1" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.19"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.4"


libraryDependencies ++= Seq(
  // testing
  // "org.scalatest"   %% "scalatest"    % "2.2.4"   % "test,it",
  //"org.scalacheck"  %% "scalacheck"   % "1.12.2"  % "test,it",
  // logging
  //"org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  //"org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // spark core
  //"org.apache.spark" % "spark-core_2.10" % "1.5.1",
  "org.apache.spark" % "spark-graphx_2.10" % "1.5.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.1",
  //"org.apache.spark" % "spark-streaming_2.10" % "1.5.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0",
  // spark packages
  "com.databricks" % "spark-csv_2.10" % "1.2.0"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

