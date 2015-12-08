import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, MapWritable}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark.rdd.{ScalaEsRDD, EsSpark}
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark._
import org.json4s.jackson.Json

/**
 * Created by kenneththomas on 11/25/15.
 */
object EsConnTest {
  val url = """src/main/resources/rddText.txt"""

  def main(args: Array[String]) {
    val conf = new org.apache.spark.SparkConf()
      .setMaster("local[*]")
      .setAppName("SprakES")
      //.set("es.nodes", "192.168.7.7")
      .set("es.nodes", "localhost") //change the IP as per your running IP of elasticsearch
      //.set("es.nodes", "192.168.1.100") //change the IP as per your running IP of elasticsearch
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .set("spark.driver.allowMultipleContexts", "true")

    implicit val sc = new org.apache.spark.SparkContext(conf)
    //implicit val sc = new SparkContext(conf)

    case class account(name: String, age: Int)

    val a1 = account("Harry", 25)
    val a2 = account("Ron", 26)

    val m1 = Map("name" -> "Ronald", "age" -> 25)
    val m2 = Map("name" -> "Dany", "age" -> 25)

    val rdd = sc.makeRDD(Seq(m1, m2))
    rdd.collect().foreach(println)
    //sc.makeRDD(Seq(json1, json2)).saveToEs("customer/external")
    EsSpark.saveToEs(rdd, "customer/external")

    connectToEs()
  }

  def connectToEs() (implicit sc: SparkContext) ={

    val bankRdd = sc.esRDD("customer/external")

    println("BankRdd: ", bankRdd)
    println("BankRdd Context: ", bankRdd.context)
    println("Bank Rdd Count",bankRdd.count())

    bankRdd.foreach(println)
  }

}
