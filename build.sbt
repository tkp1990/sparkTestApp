name := "SparkEsTemp"

version := "1.0"

scalaVersion := "2.10.4"

val scalazVersion = "7.1.0"

crossScalaVersions  := Seq("2.11.7", "2.10.4")

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.1"

libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "2.2.0-beta1" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "com.sun.jersey" % "jersey-servlet" % "1.19"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.4"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-graphx_2.10" % "1.5.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1",
  "org.apache.hadoop" % "hadoop-client" % "2.4.0",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",

  "org.scalaz" %% "scalaz-core" % scalazVersion,
  "org.scalaz" %% "scalaz-effect" % scalazVersion,
  "org.scalaz" %% "scalaz-typelevel" % scalazVersion,
  "org.scalaz" %% "scalaz-scalacheck-binding" % scalazVersion % "test"

)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

//resolvers += "fakod-snapshots" at "https://raw.github.com/FaKod/fakod-mvn-repo/master/snapshots"
