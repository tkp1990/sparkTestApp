import java.io.File

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sparkContextFunctions
import org.joda.time.DateTime

import scala.collection.immutable.HashMap

/**
 * Created by kenneththomas on 1/4/16.
 */
object LogStuff {
  val url = """src/main/resources/"""
  var map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]] = collection.mutable.HashMap()
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Ci_Log_Test")
      //.set("es.nodes", "http://acelrtech.com")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val logs = sc.esRDD("test_logs/log3")

    val d = logs.map(x => getData(x._2.get("log3").get.asInstanceOf[collection.mutable.LinkedHashMap[String, Any]]))
    d.foreach(println)

    val df = sqlContext.createDataFrame(d)
    df.registerTempTable("logs")

    sqlContext.sql(
      """
        |Select domain, logType, timeStamp, timeMilli, ip, username, week, year from logs
      """.stripMargin).show()

    df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017").groupBy("timeStamp", "domain").count().show()

    val csvColumn = df.select("domain").distinct()

    val csv = df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017").groupBy("timeStamp", "domain").count()

    generateMap(csv)
    println("CSVDATA")
    csv.foreach(println)
    map.foreach(x => println("Data: "+x))
    makeCSV(map, csvColumn)

    //df.select("domain", "logType", "timeStamp", "week", "year").where("year < 2017").groupBy("domain", "week").count().show()

    //df.select("domain", "logType", "timeStamp", "week", "year").where("year < 2017").groupBy("domain", "year").count().show()

  }

  /**
   * get case class constructed for each log entry
   * @param map
   * @return
   */
  def getData(map: collection.mutable.LinkedHashMap[String, Any]): Log = {
    val _date = map.get("datetime_ms").get.toString
    Log(getDomain(map.get("domain").get.toString), map.get("log_type").get.toString, getDate(_date).toString("yyyy-MM-dd"),
      _date.toLong, map.get("ip").get.toString, map.get("username").get.toString, getDate(_date).toString("w").toInt,
      getDate(_date).toString("yyyy").toInt)
  }

  /**
   * get domain name from the URL
   * @param domain
   * @return
   */
  def getDomain(domain: String): String ={
    domain matches ".*https://.*"
    match{
      case true =>
        val a = domain.split("\\.")
        a(0).replace("https://", "")
      case false =>
        val b = domain.split(".")
        b(0)
    }
  }

  def getDate(milli: String): DateTime = {
    new DateTime(milli.toLong * 1000)
  }

  /**
   * convert DataFrame which contains log counts for every domain grouped by date into a Map
   * @param csv
   */
  def generateMap(csv: DataFrame)= {
    for(x <- csv){
      map.contains(x.getString(0)) match {
        case true =>
          map.get(x.getString(0)).get += x.getString(1) -> x.getLong(2)
          println(map)
        case false =>
          map +=  x.getString(0) -> new collection.mutable.HashMap[String, Long]
          map.get(x.getString(0)).get += x.getString(1) -> x.getLong(2)
          println("False: ", map)
      }
    }
  }

  /**
   *
   * @param map
   * @param colsDf
   */
  def makeCSV(map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]], colsDf: DataFrame) = {
    val f = new File(url+"/out.csv")
    val writer = CSVWriter.open(f)
    val headers = colsDf.sort("domain").select("domain").rdd.map(r => r(0)).collect()
    val headerMap = headers.zipWithIndex.map(_.swap).toMap
    writer.writeRow(List("date") ++ headers.toList)
    for(x <- map.keySet){
     var list:List[String] = List(x)
     val valCol = map.get(x).get match {
       case x1:collection.mutable.HashMap[String, Long] =>
         for(a <- headerMap.keySet){
           list :::= List(x1.get(headerMap.get(a).get.toString).getOrElse("0").toString)
         }
         list
     }
     writer.writeRow(valCol.reverse)
    }
    writer.close()
  }

  /**
   * Log Case Class
   * @param domain
   * @param logType
   * @param timeStamp
   * @param ip
   */
  case class Log(domain: String, logType: String, timeStamp: String, timeMilli: Long, ip: String, username: String, week: Int, year: Int)

}
