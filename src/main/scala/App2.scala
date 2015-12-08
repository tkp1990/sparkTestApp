/**
 * Created by kenneththomas on 11/26/15.
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, MapWritable}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark._

object App2 {


  def main(args:Array[String]) = {
    val conf = new org.apache.spark.SparkConf()
      .setMaster("local[*]")
      .setAppName("SprakES")
      //.set("es.nodes", "192.168.7.7") //IP of the ElasticSearch instance running on Vagrant
      .set("es.nodes", "localhost")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .set("spark.driver.allowMultipleContexts", "true")

    implicit val spark = new org.apache.spark.SparkContext(conf)

    meth()
    //meth2()
    //getGreyLogData()
    spark.stop()
  }

  /**
   * Using esRDD to get data from ElasticSearch
   *
   * @param sc
   * @return
   */
  def meth() (implicit sc: SparkContext)= {
    val rdd = sc.esRDD("bank/account")
    //rdd.foreach(println)
    println("Rdd Count: ", rdd.count)
    val rowData = rdd.map(p => Map(p._1 -> p._2))
    rowData.foreach(p => println("Id"+ p.keys+"Value: "+p.values))
  }

  /**
   *
   * Using newAPIHadoopRDD to get data from ElasticSearch
   *
   * @param sc
   * @return
   */
  def meth2()(implicit sc: SparkContext) = {
    val conf = new Configuration()
    conf.set("es.resource", "bank/account")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    conf.set("spark.driver.allowMultipleContexts", "true")

    val newRdd = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]],
      classOf[Text], classOf[MapWritable])

    val schemaString = "accountNum balance firstname lastname age gender address"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

    println("NewRdd Count: ", newRdd.count)
    newRdd.foreach(println)
  }

  /**
   *
   * Getting data from gralogs
   *
   * @param sc
   * @return
   */
  def getGreyLogData()(implicit sc: SparkContext) ={
    val rdd = sc.esRDD("graylog2_0/message")
    //rdd.foreach(println)
    //val row = rdd.first()
    println("GreyLog Count: ",rdd.count)
  }
}
