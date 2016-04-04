name := "SparkEsTemp"

version := "1.0"

scalaVersion := "2.10.5"


//crossScalaVersions  := Seq("2.11.7", "2.10.4")

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"

libraryDependencies ++= Dependencies.sparkAkkaHadoop


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-graphx_2.10" % "1.5.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1",
  "org.apache.poi" % "poi" % "3.9",
  "org.mongodb" % "mongo-java-driver" % "3.0.4",
  "org.mongodb" %% "casbah" % "3.1.0",
  "com.typesafe.play" % "play-json_2.10" % "2.4.6" exclude("com.fasterxml.jackson.core", "jackson-databind")

)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

initialCommands in console :=
  """
    |import org.apache.spark._
    |import org.apache.spark.streaming._
    |import org.apache.spark.streaming.StreamingContext._
    |import org.apache.spark.streaming.dstream._
    |import akka.actor.{ActorSystem, Props}
    |import com.typesafe.config.ConfigFactory
    |import org.elasticsearch.spark._
  """.stripMargin