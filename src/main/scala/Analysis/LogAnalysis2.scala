package Analysis

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.elasticsearch.spark.sparkContextFunctions
import org.joda.time.DateTime
import Analysis.Constants._

/**
 * Created by kenneththomas on 1/13/16.
 */
object LogAnalysis2 {
  var map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]] = collection.mutable.HashMap()

  def main(args: Array[String]) {
    val m = analyzeLogs("local", "9200", "test_logs/log3")
    m.foreach(println)
  }

  /**
   * Analyze Logs provides log data for log Type spread on a specified date range
   *
   * @param node - Elastic Search node
   * @param port - Elastic Search Port
   * @param index - Elastic Search Index(eg: ci_logs/log)
   * @return
   */
  def analyzeLogs(node: String, port: String, index: String):collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]]  = {
    val esConf = ESConfig.getEsConfig(node, port)
    val conf = SparkConfig.getSparkConfig(esConf)

    val sc = SparkConfig.getSparkContext(conf)
    val sqlContext = new SQLContext(sc)//SparkConfig.getSqlContext(sc)
    val logs = sc.esRDD(index)
    val indexMapping = index.split("\\/")
    val d = logs.map(x => getData(x._2.get("log3").get.asInstanceOf[collection.mutable.LinkedHashMap[String, Any]]))
    println("Data returned from ES")
    d.foreach(println)
    val df = sqlContext.createDataFrame(d)
    df.registerTempTable("logs3")
    sqlContext.sql(
      """
        |Select domain, logType, timeStamp, timeMilli, ip, username, week, year from logs3
      """.stripMargin).show()

    df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017")
      .groupBy("timeStamp", "domain").count().show()
    val csvColumn = df.select(Constants.LOG_DOMAIN).distinct()
    val csv = df.select(LOG_DOMAIN, LOG_TYPE, LOG_TIMESTAMP, LOG_YEAR).sort(LOG_TIMESTAMP).where("year < 2017")
      .groupBy(LOG_TIMESTAMP, LOG_DOMAIN).count()

    generateMap(csv)

    sc.stop()
    map
  }

  def getJson(): Unit ={

  }



  /**
   * Takes the log _source JSON, extracts the Data and makes it into a LOG instance
   * @param map
   * @return
   */
  def getData(map: collection.mutable.LinkedHashMap[String, Any]): Log = {
    val _date = map.get(DATETIME_MS).get.toString
    Log(getDomain(map.get(LOG_DOMAIN).get.toString), map.get(LOG_TYPE).get.toString,
      getDate(_date).toString(DATE_FORMAT_NORMAL), _date.toLong, map.get(LOG_IP).get.toString,
      map.get(USERNAME).get.toString, getDate(_date).toString(DATE_FORMAT_WEEK).toInt,
      getDate(_date).toString(DATE_FORMAT_YEAR).toInt)
  }

  /**
   * Returns only the domain value
   *
   * @param domain - Raw Domain value (eg: https://domain.com)
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

  /**
   * Converts time to milliseconds to be converted to Date Format
   *
   * @param milli
   * @return
   */
  def getDate(milli: String): DateTime = {
    new DateTime(milli.toLong * 1000)
  }

  /**
   * convert DataFrame which contains log counts for every domain grouped by date into a Map
   *
   * @param csv - Raw unstructured data to be converted to a structured Map
   */
  def generateMap(csv: DataFrame)= {
    for(x <- csv){
      map.contains(x.getString(0)) match {
        case true =>
          map.get(x.getString(0)).get += x.getString(1) -> x.getLong(2)
        case false =>
          map +=  x.getString(0) -> new collection.mutable.HashMap[String, Long]
          map.get(x.getString(0)).get += x.getString(1) -> x.getLong(2)
      }
    }
  }

}
