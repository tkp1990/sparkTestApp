/**
 * Created by kenneththomas on 12/31/15.
 */

import java.text.SimpleDateFormat

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.rdd.{ScalaEsRDD, EsSpark}
import org.joda.time.DateTime

import scala.collection.mutable

/**
 * Created by kenneththomas on 12/29/15.
 */
object OrderAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Ci_Order_Test")
      .set("es.nodes", "acelrtech.com")
      //.set("es.nodes", "localhost")
      .set("es.port", "9400")

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
        |Select id, index, orderType, createdDate, week, year from orders
      """.stripMargin).show()

    /**
     * orders grouped by year
     */
    dF.groupBy("orderType", "createdDate").count.show()

    sqlContext.sql(
    """
      |Select createdDate, count(orderType) from orders where orderType = 'order' GROUP BY createdDate
      |""".stripMargin
    ).show()

    /**
     * Orders grouped by week
     */
    dF.select("createdDate", "week", "orderType").where("orderType = 'order'").groupBy("week").count().show()

    /**
     * orders grouped by year(YTD)
     */
    dF.select("createdDate", "year", "orderType").where("orderType = 'order'").groupBy("year").count().show()

  }

  def getOrdersData(map: collection.mutable.LinkedHashMap[String, Any]): Order={
    map.get("_type").get.toString.equals("order")
    match {
      case true =>
        val _date = new DateTime(map.get("_source").get.asInstanceOf[mutable.LinkedHashMap[String, Any]]
          .get("created_on").get.toString.toLong * 1000)
        Order(map.get("_id").get.toString, map.get("_index").get.toString, map.get("_type").get.toString,
          _date.toString("dd-MM-yyyy"), _date.toString("w").toInt, _date.toString("yyyy").toInt)
      case false =>
        Order(map.get("_id").get.toString, map.get("_index").get.toString, map.get("_type").get.toString,
          new DateTime(0).toString("dd-MM-yyyy"), 0, 0)
    }
  }

  def getLine(map: collection.mutable.LinkedHashMap[String, Any]): Unit ={

  }

  trait BaseOrder{
    def id: String
    def index: String
    def orderType: String
  }
  case class Order(id: String, index: String, orderType: String,createdDate: String, week: Int, year: Int)
  case class SapOrder(id: String, index: String, orderType: String) extends BaseOrder
  case class Line(name: String, qyt: Double)
}

