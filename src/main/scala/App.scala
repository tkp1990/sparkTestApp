/**
 * Created by kenneththomas on 11/24/15.
 */

import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.rdd.EsSpark


object App {

  def main (args: Array[String]){
    val conf = new SparkConf().setAppName("SparkTestApp").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)
    val url = """src/main/resources/accounts.json"""
    case class account(firstname: String, balance: Int, acc_number: String)

    val acc1 = account("Ken", 10000, "12345")
    val acc2 = account("Kevin", 10000,"OT1234")

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    getJsonData(url)
    getCount()
    /*
    getCount()
    val rdd = sc.makeRDD(Seq(acc1, acc2))
    println("Rdd:", rdd.count())
    rdd.collect().foreach(println)
    EsSpark.saveToEs(rdd, "bank/account")
*/
  }

  def getCount() (implicit sc: SparkContext)= {
    val conf = new JobConf()
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.resource", "bank/account")
    val esRDD = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]],
      classOf[Text], classOf[MapWritable])
    print("ES: ",esRDD)
    val d = new SQLContext(sc)
    val docCount = esRDD.count();
    println("Doc Count: ",docCount)

  }

  def getFileData(url: String) (implicit sc: SparkContext) = {
    val file = sc.textFile(url)
  }

  def getJsonData(url: String) (implicit sc: SQLContext, s: SparkContext)  = {
    val bankDetails = sc.read.json(url)
    //sc.parallelize(1 to 1000000).collect().filter(_ < 1000)
    bankDetails.printSchema()
    bankDetails.registerTempTable("bankDetails")
    val pairs = bankDetails.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    counts.collect().foreach(println)
    /**
     * Generating a SchemaRDD from Json Data
     */

    val accountRDD = s.parallelize(
      """{"account_number":"1234","address":"6301 Fifth Ave", "age":25,"balance":10000,
        |"city": "Pittsburgh"}""".stripMargin :: Nil)
    val account = sc.read.json(accountRDD)
    println("Generated SchemaRDD: ",account)
  }

  import spray.json._
  import scala.collection.immutable.{SortedMap, HashMap}
  import scala.collection.mutable

  object M2eeJsonProtocol extends DefaultJsonProtocol {

    implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] { // 1
    def write(m: Map[String, Any]) = {
      JsObject(m.mapValues {                  // 2
        case v: String => JsString(v)         // 3
        case v: Int => JsNumber(v)
        case v: Map[_, _] => write(v.asInstanceOf[Map[String, Any]])  // 4
        case v: Any => JsString(v.toString)   // 5
      })
    }

      def read(value: JsValue) = ???            // 6
    }
  }
}

