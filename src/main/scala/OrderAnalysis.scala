/**
 * Created by kenneththomas on 12/31/15.
 */

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.rdd.{ScalaEsRDD, EsSpark}
import org.joda.time.DateTime
import org.json.JSONObject

import scala.collection.mutable
import scala.util.parsing.json.JSONArray

/**
 * Created by kenneththomas on 12/29/15.
 */
object OrderAnalysis {
  var map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]] = collection.mutable.HashMap()

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Ci_Order_Test")
      //.set("es.nodes", "acelrtech.com")
      .set("es.nodes", "localhost")
      .set("es.port", "9200")
      .set("query", """{"query":{"match":{"_type":"order"}}}""")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val orders = sc.esRDD("ci_orders/order")
    orders.foreach(println)
    val d = orders.map(x => getOrdersData(x._2.get("order").get.asInstanceOf[collection.mutable.LinkedHashMap[String, Any]]))
    d.foreach(println)

    val dF = sqlContext.createDataFrame(d)
    dF.registerTempTable("orders")

    sqlContext.sql(
      """
        |Select username, order_no, status_code, orderType, domain, createdDate, week, year from orders
        |where orderType='order'
      """.stripMargin).show()

    val colsDomain = dF.select("domain").where("domain <> '' ").distinct()
    val colsUsernames = dF.select("username").where("username <> '' ").distinct()
    val headers = colsUsernames.sort("username").select("username").rdd.map(r => r(0)).collect()
    dF.select("username", "order_no", "status_code", "domain", "orderType", "createdDate")
      .where("orderType = 'order'")
      .orderBy("createdDate")
      .groupBy("username","createdDate").count().show()
    val a = dF.select("username", "order_no", "status_code", "domain", "orderType", "createdDate")
      .where("orderType = 'order'")
      .orderBy("createdDate")
      .groupBy("createdDate", "username").count()


    dF.select("username", "order_no", "status_code", "domain", "orderType", "createdDate")
      .where("orderType = 'order'")
      .orderBy("createdDate")
      .groupBy("createdDate", "domain").count().show()


    generateMap(a)
    map.foreach(println)
    val b = makeCsvJson(map, headers.toList.asInstanceOf[List[String]])
    println(b)
    /**
     * orders grouped by year
     */
    //dF.groupBy("orderType", "createdDate").count.show()

    /**
     * Orders grouped by week
     */
    //dF.select("createdDate", "week", "orderType").where("orderType = 'order'").groupBy("week").count().show()

    /**
     * orders grouped by year(YTD)
     */
    //dF.select("createdDate", "year", "orderType").where("orderType = 'order'").groupBy("year").count().show()

  }

  def getOrdersData(mainMap: collection.mutable.LinkedHashMap[String, Any]): Order={
    val map = mainMap.get("_source").get.asInstanceOf[collection.mutable.LinkedHashMap[String, Any]]
    try{
      val _date = new DateTime(map.get("created_on").get.toString.toLong * 1000)
      Order(map.get("username").get.toString,map.get("order_no").get.toString, map.get("status_code").get.toString, mainMap.get("_type").get.toString,
        mainMap.get("_index").get.toString, _date.toString("dd-MM-yyyy"), _date.toString("w").toInt, _date.toString("yyyy").toInt)
    }catch{
      case e:Exception =>
        Order(map.get("username").getOrElse("None").toString,map.get("order_no").getOrElse("None").toString, map.get("status_code").getOrElse("Nonde").toString,
          mainMap.get("_type").getOrElse("None").toString,
          mainMap.get("_index").getOrElse("None").toString, "1970-01-01", 0, 0)
    }
  }

  def getLine(map: collection.mutable.LinkedHashMap[String, Any]): Unit ={

  }

  def generateMap(csv: DataFrame)= {
    val m = List
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

  def makeCsvJson(map: collection.mutable.HashMap[String, collection.mutable.HashMap[String, Long]], headers: List[String]): JSONObject ={
    val json = new JSONObject()
    try{
      //      val headers = colsDf.sort("domain").select("domain").rdd.map(r => r(0)).collect()
      val headerMap = headers.zipWithIndex.map(_.swap).toMap
      val jsonHeader = new JSONArray(headers.toList)
      json.put("header", jsonHeader)
      val dataJson = new JSONObject()
      var a = map.keySet

      for( x <- map.keySet){
        if(!x.equals("")){
          var list:List[String] = List()
          val valCol = map.get(x).get match {
            case x1:collection.mutable.HashMap[String, Long] =>
              for(a <- headerMap.keySet){
                list :::= List(x1.get(headerMap.get(a).get.toString).getOrElse("0").toString)
              }
              list
          }
          val jsonVal = new JSONArray(valCol.reverse)
          dataJson.put(x,jsonVal)
        }
      }
      json.put("data", dataJson)
      json
    }catch{
      case e:Exception =>
        e.printStackTrace()
        json.put("error", e.getMessage)
    }
  }

  trait BaseOrder{
    def id: String
    def index: String
    def orderType: String
  }
  case class Order(username: String, order_no: String, status_code: String, orderType: String,domain: String, createdDate: String, week: Int, year: Int)
  case class SapOrder(id: String, index: String, orderType: String) extends BaseOrder
  case class Partner(name: String, address: String, email: String)
}

