/**
 * Created by kenneththomas on 12/1/15.
 */

import org.elasticsearch.spark.{sparkContextFunctions, sql}

object LogParse {

  val url = """src/main/resources/"""
  def main(args: Array[String]) {
    val conf = new org.apache.spark.SparkConf()
      .setMaster("local[*]")
      .setAppName("SprakES")
      //.set("es.nodes", "192.168.7.7") //IP of the ElasticSearch instance running on Vagrant
      .set("es.nodes", "localhost")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .set("spark.driver.allowMultipleContexts", "true")

    implicit val sc = new org.apache.spark.SparkContext(conf)
    val rdd = sc.esRDD("graylog2_0/message")
    println("Rdd Count: ", rdd.count)
    //rdd.foreach(println)
    println(rdd.first())

    //val rowData = rdd.map(p => Map(p._1 -> p._2))
  }

}
