import java.util

import com.fasterxml.jackson.annotation.JsonValue
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jgrapht.Graph
import play.api.libs.json.JsObject
import play.api.libs.json.Json

import scala.StringBuilder
import scala.collection.immutable.{HashMap, SortedMap}
import scala.collection.mutable

/**
 * Created by kenneththomas on 11/30/15.
 */
object LogAnalysis {

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
      .set("spark.executor.memory", "2g")
      .set("SPARK_EXECUTOR_CORES", "2")
      //SPARK_EXECUTOR_CORES
    implicit val p = new AccessLogParser()
    implicit val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new SQLContext(sc);

    val accessLog = sc.textFile(url+"access_log")
    //accessLog.map(line => makeMap(p.parseRecord(line))).map()
    //accessLog.foreach(println)

    println("-----------------------------------------------------------------")

    //accessLog.foreach(a => makeMap(a))
    val accesLogCount = accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").count()
    println("Count: ", accesLogCount)

//    accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").map(getRequest(_)).count
//    val recs = accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").map(getRequest(_))
//    val distRecs = accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").map(getRequest(_)).distinct()
//    distRecs.foreach(println)

    println("No of requests made per Ip: ")
    val ips = accessLog.map(line => getIp(p.parseRecord(line))).map(ip => (ip, 1)).reduceByKey(_ + _)
    //ips.foreach(println)
    val ip = ips.map(x => ipClass(x._1.toString, x._2.toInt))
    val df = sqlContext.createDataFrame(ip)
    //df.write.json(url+"ips")
    df.registerTempTable("ips")
    /*sqlContext.sql(
      """
        |Select ips, pings from ips
      """.stripMargin).show()*/

    println("No of request Types: ")
    val requests = accessLog.map(line => requestType(p.parseRecord(line))).map(req => (req, 1)).reduceByKey(_ + _)
    //requests.foreach(println)

    println("No of Status Codes Grouped: ")
    val statusCodes = accessLog.map(line => getAccessCode(p.parseRecord(line))).map(code => (code, 1)).reduceByKey(_ + _)
    statusCodes.foreach(println)
    val c = statusCodes.map(x => codes(x._1.toString, x._2.toInt))
    val codeDf = sqlContext.createDataFrame(c)
    codeDf.write.json(url+"code")

    println("Request url:")

    val requesturl = accessLog.map(line => getRequestURL(p.parseRecord(line))).filter(x => x.toString.contains("/"))
    implicit var count1: scala.collection.mutable.HashMap[String, Int] = new scala.collection.mutable.HashMap[String, Int]()
    implicit var pc: scala.collection.mutable.HashMap[String, util.ArrayList[String]] =
      new scala.collection.mutable.HashMap[String, util.ArrayList[String]]()
    pc.put("root", new util.ArrayList[String])
    count1.put("root", 0)

    val mainList: RDD[List[String]] = requesturl flatMap ( r => r.toString split("\\?") map (x => parser(x.split("/").filter(x => !x.contains("=")).toList).valuesIterator.toList))
    mainList.foreach(println)
    val returnData = getData(mainList)
    returnData.foreach(println)
//    for(r <- requesturl){
//      val obj = r.toString.split("\\?")
//      obj.map(x => parser(x.split("/").filter(x => !x.contains("=")).toList).valuesIterator.toList)//.foreach(a => println(a.toString)))
//    }
    println("MainList: "+mainList)
    for(a <- pc.keySet){
      //println("Key: "+ a.toString +" -> "+ pc.get(a).get)
      val list = pc.get(a.toString)

    }
    for(a <- count1.keySet){
      //println(a +" -> "+ count1.get(a))
    }

  }

  def parser(list: List[String]): Map[Int, String]= {
    val m = list.zipWithIndex.map(_.swap).toMap
    val sM = SortedMap(m.toSeq:_*)
    sM.+(0 -> "root")
  }

  def getData(input: RDD[List[String]]): mutable.HashMap[String, innerMap] ={
    var mainMap = new mutable.HashMap[String, innerMap]
    //mainMap + input.foreach(x => (storeData(x.toIterator, mainMap ,x(0)).toMap)).asInstanceOf
    for(x <- input){
      val z: mutable.HashMap[String, innerMap] = storeData(x.toIterator, mainMap ,x(0).toString)
      println("Next Map",z)
      /*val a = merge(mainMap.asInstanceOf[innerMap], z.asInstanceOf[innerMap])
      println("Merged maps",a)*/
      val mainSet = getKeys(mainMap.asInstanceOf[innerMap]).zipWithIndex.map(_.swap).toMap
      println("MainSet: ",mainSet)
      val zSet = getKeys(z.asInstanceOf[innerMap]).zipWithIndex.map(_.swap).toMap
      println("Zset: ",zSet)
      for(a <- mainSet.keySet) {
        var parent = "root"
        zSet.get(a).isDefined match {
          case true =>
            (mainSet.get(a).equals(zSet.get(a))) match {
              case false =>
                zSet.size > 0 match {
                  case true =>
                    mainMap.update("root", z.get(zSet.get(a).get).asInstanceOf[innerMap])
                }
            }
        }
        parent = mainSet.get(a).getOrElse("root")
      }
      //mainMap.update("root", a.asInstanceOf[innerMap])
      println("Main Map Modified", mainMap)
    }
    mainMap
  }

  def getKeys(map: innerMap): List[String] ={
    var set = mutable.ArrayStack[String]()
    val keySet = map.keySet
    if(keySet.size > 0) {
      for (a <- map.keySet) {
        println("a: ",a)
        set += a
        val x = getKeys(map.get(a).get.asInstanceOf[innerMap])
        for(y <- x ){
          println("Y ",y)
          //if(!set.contains(y))
          //set += y
        }
        if(!set.contains(a))
          set += a.toString
      }
    }
    println("Set: ",set)
    set.toList
  }

  type innerMap = HashMap[String, Any]

  def storeData(list: Iterator[String], map: mutable.HashMap[String, innerMap], root: String): mutable.HashMap[String, innerMap]={
    list.hasNext match {
      case true =>
        val v = list.next()
        val y = map contains (root) match {
          case true =>
            println("Adding when exists: "+v)
            val childMap = map.get(v).get match {
              case _:HashMap[String, Any] => asInstanceOf[mutable.HashMap[String, innerMap]]
              case _ => new mutable.HashMap[String, innerMap]
            }
            val x = map + (v -> storeData(list, childMap, v))
            x
          case false =>
            val x = map + (v -> storeData(list, new mutable.HashMap[String, innerMap], v))
            x
        }
        y.asInstanceOf[mutable.HashMap[String, innerMap]]
      case false =>
        new mutable.HashMap[String, innerMap]
    }
  }

  def getRequestURL(line: Option[AccessLogRecord]) = {
    line match{
      case Some(l) => l.requestURL
      case none => ""
    }
  }

  def getAccessCode(line :Option[AccessLogRecord]) = {
    line match{
      case Some(l) => l.httpStatusCode
      case None => 0
    }
  }

  def getIp(line :Option[AccessLogRecord]): String = {
    line match{
      case Some(l) => l.clientIpAddress
      case None => ""
    }
  }

  def makeMap(line: String) (implicit p: AccessLogParser) =  {
    val data = p.parseRecord(line)
    //println(data)
    data match{
      case Some(l) => {
        println(l.httpStatusCode)
        //(l.httpStatusCode, data)
      }
      case None => {
        println("nothing here at all")

      }
    }
  }

  def requestType(line: Option[AccessLogRecord]): String = {
    line match{
      case Some(l) => l.request
      case None => ""
    }
  }

  def getRequest(accessString: String) (implicit p: AccessLogParser) : Option[String] = {
    val a = p.parseRecord(accessString)
    a match {
      case Some(rec) => Some(rec.request)
      case None => None
    }
  }

  case class ipClass(ips: String, pings: Integer)

  case class codes(code: String, count: Integer)
}
//var myData = JSON.parse(ips); get json file in HTML