import java.io.File
import java.util.logging.Logger


import com.github.tototoshi.csv.CSVWriter
import org.apache.hadoop.io.{Text, MapWritable}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark.sparkContextFunctions
import org.joda.time.DateTime
import org.json.JSONObject


import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.util.parsing.json.JSONArray

/**
 * Created by kenneththomas on 1/4/16.
 */
object LogStuff {
  val url = """src/main/resources/"""
  var map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]] = collection.mutable.HashMap()
  val logLevelsMap: collection.mutable.HashMap[String, collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]]] = collection.mutable.HashMap()
  def main(args: Array[String]) {

    /**
     * {
          "query": {
            "filtered": {

                "filter": {
                  "range": {
                    "datetime_utc": {
                        "gte": "2014-12-31 00:00:00",
                        "lte":"2016-12-31 00:00:00",
                        "format": "strict_date_optional_time||epoch_millis"
                    }
                }
                }
            }
          }
        }
     */
    //val query = """{"query": { "filtered": { "filter": { "range": { "datetime_utc": { "gte": "2014-12-31 00:00:00", "lte":"2016-12-31 00:00:00", "format": "strict_date_optional_time||epoch_millis"}}}}}}"""
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Ci_Log_Test")
      .set("es.nodes", "http://acelrtech.com")
      //.set("es.nodes", "localhost")
      .set("es.port", "9400")
      //.set("es.query" , query)

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val logs = sc.esRDD("ci_logs/log")
    //val logs = sc.hadoopRDD(conf,classOf[EsInputFormat[Text, MapWritable]],classOf[Text], classOf[MapWritable])
    //logs.foreach(println)
    val d = logs.map(x => getData(x._2.get("log").get.asInstanceOf[collection.mutable.LinkedHashMap[String, Any]]))
    //d.foreach(println)

    val df = sqlContext.createDataFrame(d)
    df.registerTempTable("logs")

    sqlContext.sql(
      """
        |Select domain, logType, timeStamp, timeMilli, ip, username, week, year from logs
      """.stripMargin).show()

//    df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017").groupBy("timeStamp", "domain").count().show()

//    val csvColumn = df.select("domain").distinct()
//
//    val csv = df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017").groupBy("timeStamp", "domain").count()
//
//    generateMap(csv)
//    println("CSVDATA")
//    csv.foreach(println)
//    map.foreach(x => println("Data: "+x))
//    makeCSV(map, csvColumn)

    //df.select("domain", "logType", "timeStamp", "week", "year").where("year < 2017").groupBy("domain", "week").count().show()

    //df.select("domain", "logType", "timeStamp", "week", "year").where("year < 2017").groupBy("domain", "year").count().show()


    //Log Levesl logic

    df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017 and domain <> '' ").orderBy("timeStamp").groupBy("domain", "timeStamp", "logType").count().show()
    //val csvColumn = df.select("domain").distinct()
    val logsLevelData = df.select("domain", "logType", "timeStamp", "year").sort("timeStamp").where("year < 2017 and domain <> '' and timeStamp <> ''").orderBy("timeStamp").groupBy("domain", "timeStamp", "logType").count()
    val csvColumn = df.select("domain").where("domain <> '' ").distinct()
    val headers = csvColumn.sort("domain").select("domain").rdd.map(r => r(0)).collect()
    val typeCols = df.select("logType").where("logType <> '' ").distinct()
    val typeList = typeCols.sort("logType").select("logType").rdd.map(r => r(0)).collect()
    generateLogLevelsMap(logsLevelData)
    println("Final Map::")
    logLevelsMap.foreach(println)
    val ret = makeCsvJsonLogLevel(logLevelsMap, headers.toList.asInstanceOf[List[String]], typeList.toList.asInstanceOf[List[String]])
    println(ret)
  }

  def makeCsvJsonLogLevel(map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]]],
                          headers: List[String], typeList: List[String]): JSONObject = {
    val json = new JSONObject()
    var message = "";
    try{
      val _typemap = typeList.zipWithIndex.toMap
      val _headerMap = headers.zipWithIndex.toMap
      val domainJson = new JSONObject()
      var domainList:List[JSONObject] = List()
      for(x <- _headerMap.keySet) {
        if(map.contains(x)){
          val dateMap = map.get(x).get
          var dateListJson: List[JSONObject] = List()
          for (y <- dateMap.keySet) {
            val dateLogLevel = new JSONObject()
            val logLevel = dateMap.get(y).get
            val log = new JSONObject()
            for (z <- _typemap.keySet) {
              log.put(z, logLevel.get(z).getOrElse(0))
            }
            dateLogLevel.put(y, log)
            dateListJson :::= List(dateLogLevel)
          }
          val dateListArray = new JSONArray(dateListJson)
          //domainList :::= dateListJson
          domainJson.put(x,dateListArray)
        }else {
          message + "Specified Customer"+ x + "does not exist in the Logs"
        }
      }
      domainList :::= List(domainJson)
      val jsonArray = new JSONArray(domainList)
      jsonArray
      json.put("data", jsonArray)
      json.put("message", message)
      json
    }catch {
      case e:Exception =>
        e.printStackTrace()
        json.put("error", e.getMessage)
    }
  }

  /**
   * get case class constructed for each log entry
   * @param map
   * @return
   */
  def getData(map: collection.mutable.LinkedHashMap[String, Any]): Log = {
    try{
      val _date = map.get(Constants.DATETIME_MS).get.toString
      Log(getDomain(map.get(Constants.LOG_DOMAIN).get.toString), map.get(Constants.LOG_TYPE).get.toString,
        getDate(_date).toString(Constants.DATE_FORMAT_NORMAL), _date.toLong, map.get(Constants.LOG_IP).get.toString,
        map.get(Constants.USERNAME).get.toString, getDate(_date).toString(Constants.DATE_FORMAT_WEEK).toInt,
        getDate(_date).toString(Constants.DATE_FORMAT_YEAR).toInt)
    }catch{
      case e:Exception =>
        println("Bad Data, DataTime not present in Received Json")
        try{
          Log.apply(getDomain(map.get(Constants.LOG_DOMAIN).get.toString), map.get(Constants.LOG_TYPE).get.toString,
            map.get(Constants.LOG_IP).get.toString,
            map.get(Constants.USERNAME).get.toString)
        }catch{
          case e:Exception =>
            println("Bad Data, unable to parse received Json from Elastic Search", e)
            Log("","","",0,"","",0, 0)
        }
    }
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

  type innerMap =  mutable.HashMap[String, Long]
  def generateLogLevelsMap(data: DataFrame): mutable.HashMap[String, mutable.HashMap[String, innerMap]]={
    for(x <- data){
      logLevelsMap.contains(x.getString(0)) match {
        case true =>
          var dateMap = logLevelsMap.get(x.getString(0)).getOrElse(new mutable.HashMap[String, innerMap]())
          dateMap.contains(x.getString(1)) match {
            case true =>
              var logMap = dateMap.get(x.getString(1)).getOrElse(new mutable.HashMap[String, Long]())
              logMap.contains(x.getString(2)) match {
                case false =>
                  logMap.put(x.getString(2), x.getLong(3))
              }
              dateMap.update(x.getString(1), logMap)
            case false =>
              var logMap = new mutable.HashMap[String, Long]()
              logMap put (x.getString(2), x.getLong(3))
              dateMap put (x.getString(1), logMap)
          }
          logLevelsMap.update(x.getString(0), dateMap)
        case false =>
          var dateMap = logLevelsMap.get(x.getString(0)).getOrElse(new mutable.HashMap[String, innerMap]())

          var logMap = new mutable.HashMap[String, Long]()
          logMap.put(x.getString(2), x.getLong(3))
          dateMap put (x.getString(1), logMap)

          logLevelsMap put (x.getString(0) , dateMap)
      }
    }
    logLevelsMap
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


  object Log{
    def apply(domain: String, log_type: String, ip: String, username: String): Log = {
      Log(domain, log_type, "", 0, ip, username, 0, 0)
    }
  }
}
