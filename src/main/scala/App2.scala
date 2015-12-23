/**
 * Created by kenneththomas on 11/26/15.
 */

import LogAnalysis.innerMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, MapWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark._
import spray.json._

import scala.collection.immutable.{SortedMap, HashMap}
import scala.collection.mutable
import scalaz._

object App2 extends DefaultJsonProtocol{

  val url = """src/main/resources/"""
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
    implicit val p = new AccessLogParser()

    val a1 = collection.mutable.HashMap("root" -> collection.mutable.HashMap("twiki" -> collection.mutable.HashMap("bin" -> collection.mutable.HashMap
    ("view" -> collection.mutable.HashMap("Main" -> collection.mutable.HashMap("PeterThoeny" -> collection.mutable.HashMap()))))))

    val b2 = collection.mutable.HashMap("root" -> collection.mutable.HashMap("twiki1" -> collection.mutable.HashMap("bin1" -> collection.mutable.HashMap
    ("view" -> collection.mutable.HashMap("Main1" -> collection.mutable.HashMap("PeterThoeny1" -> collection.mutable.HashMap()))))))

    val m = merge(a1.asInstanceOf[innerMap], b2.asInstanceOf[innerMap])
    m.foreach(println)


    val accessLog = spark.textFile(url+"testlog")
    val requesturl = accessLog.map(line => getRequestURL(p.parseRecord(line))).filter(x => x.toString.contains("/"))

    implicit var count: scala.collection.mutable.HashMap[String, Int] = new scala.collection.mutable.HashMap[String, Int]()

    val mainList: RDD[List[String]] = requesturl flatMap ( r => r.toString split("\\?") map (x => parser(x.split("/").filter(x => !x.contains("=")).toList).valuesIterator.toList))
    val returnData = getData(mainList)
    println("---------------------------------Return data-------------------------------")
    returnData.foreach(println)
    makeJSON(returnData.asInstanceOf[innerMap])
    //meth()
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

    /*val schemaString = "accountNum balance firstname lastname age gender address"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

    println("NewRdd Count: ", newRdd.count)
    newRdd.foreach(println)*/


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

  def getRequestURL(line: Option[AccessLogRecord]) = {
    line match{
      case Some(l) => l.requestURL
      case none => ""
    }
  }

  def parser(list: List[String]): Map[Int, String]= {
    val m = list.zipWithIndex.map(_.swap).toMap
    val sM = SortedMap(m.toSeq:_*)
    sM.+(0 -> "root")
  }

  def getCount(input: RDD[List[String]]) = {
    val count = input.flatMap(line => line.map(word => (word, 1)).toMap).reduceByKey(_ + _)
  }

  type innerMap = mutable.HashMap[String, Any]
  var mainMap: mutable.HashMap[String, innerMap] = new mutable.HashMap[String, innerMap]
  def getData(input: RDD[List[String]]): mutable.HashMap[String, innerMap] ={
    for(x <- input){
      val z: mutable.HashMap[String, innerMap] = storeData(x.toIterator, mainMap ,x(0).toString)
      mainMap = merge(mainMap.asInstanceOf[innerMap], z.asInstanceOf[innerMap]).asInstanceOf[mutable.HashMap[String, innerMap]]
      println(mainMap)
      mainMap
    }
    mainMap
  }

  def merge( me : innerMap, you : innerMap ) : innerMap = {
     val keySet = me.keySet ++ you.keySet;
     def nodeForKey( parent : innerMap, key : String ) : innerMap =
       parent.getOrElse( key, mutable.Map.empty ).asInstanceOf[innerMap]
       collection.mutable.Map(keySet.map( key => (key -> merge( nodeForKey(me, key), nodeForKey(you, key)))).toMap.toSeq:_*).asInstanceOf[innerMap]
  }

  def combineMaps(map1 : mutable.HashMap[String, innerMap], map2 : mutable.HashMap[String, innerMap]): mutable.HashMap[String, innerMap] ={
    val keys = map1.keySet ++ map2.keySet
    for(k <- keys){
      val x = map1.get(k) match {
        case Some(xx) =>
          if(xx.isInstanceOf[HashMap[String, innerMap]])
            combineMaps(map1.get(k).get.asInstanceOf[mutable.HashMap[String, App2.innerMap]], map2.get(k).get.asInstanceOf[mutable.HashMap[String, App2.innerMap]])
        case None =>
          map1.put(k,map2.get(k).get)
          map1.get(k).get
      }
      map1.update(k, x.asInstanceOf[App2.innerMap])
    }
    map1.asInstanceOf[mutable.HashMap[String ,innerMap]]
  }

  def storeData(list: Iterator[String], map: mutable.HashMap[String, innerMap], root: String): mutable.HashMap[String, innerMap]={
    list.hasNext match {
      case true =>
        val v = list.next()
        val y = map contains (v) match {
          case true =>
            val childMap = map.get(v).getOrElse(new mutable.HashMap[String, innerMap]).asInstanceOf[mutable.HashMap[String, innerMap]]
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

  var jSon: List[String] = List.empty[String]
  def makeJSON(map: innerMap) ={
    val a = MapJsonFormat.write(collection.immutable.Map(map.toSeq: _*))
    println(a)
  }

  var ret: String = ""
  def formatMap(map: innerMap, flag: Boolean, bracketCount: Int)(implicit count: Map[String, Int]): String ={

    map.keySet.size > 0 match {
      case true =>
        for(k <- map.keySet){
          ret += ("\"name\":" + "\"" +k+ "\"")
          ret += (",")
          ret += ("\"children\":[")
          ret += formatMap(map.get(k).get.asInstanceOf[innerMap], false, 0)
        }
        ret
      case false =>
        for(k <- map.keySet){
          ret += "{"
          ret += ("\"name\":" + "\"" + k + "\"")
          ret += (",")
          ret += ("\"size\":" + "\"" +count.get(k)+ "\"")
          ret += "}"
        }
        ret
    }
  }

  trait parentChild{
    type T
    val name: String
  }

  case class parent( name:String, children: Seq[parentChild]) extends parentChild
  case class child( name:String, size: Integer) extends parentChild

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {

    def write(m: Map[String, Any]) = {
      JsObject(m.mapValues {
        case v: String => JsString("name:\""+v+"\"")
        case v: Int => JsNumber(v)
        case v: Map[String, Any] => write(v.asInstanceOf[Map[String, Any]])
        case v: Any => write(collection.immutable.Map(v.asInstanceOf[mutable.HashMap[String, Any]].toSeq: _*))
      })
    }
    def read(value: JsValue) = ???
  }
}

