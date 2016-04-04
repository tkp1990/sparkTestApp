package mongoEsPush

import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkContext, SparkConf}
import play.api.libs.json.{JsValue, Json, JsObject}
import org.elasticsearch.spark._
/**
 * Created by kenneththomas on 4/4/16.
 */
object PushData {
  def main(args: Array[String]) {
    getData()
  }

  def addToEs(jsList: List[JsObject]) = {
    val conf = new SparkConf().setAppName("MongoToEs").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)
    try{
      for( x <- jsList) {
        println(x)
        sc.makeRDD(Seq(x)).saveToEs("supplier/data")
      }
    } catch {
      case e: Exception => println("Exception: " + e.getMessage)
    } finally {
      sc.stop()
    }
  }

  def getData() = {
    val finalCount = 52982819
    var skip = 0
    val limit = 5000
    while (finalCount >= skip ) {
      val mongoClient = getMongoClient("localhost", 27017)
      val (collection, mdbClient) = getCollection("myDb", "myCollection", mongoClient)
      try {
        val data = collection.find().skip(skip).limit(limit)
        skip = skip + limit
        var jsonList: List[JsObject] = List[JsObject]()
        for(x <- data) {
          val json = Json.parse(x.toString);
          val supplier = (json \ "value").as[JsValue]
          val jObj = Json.obj("data" -> supplier)
          //println(json.toString())
          jsonList = jObj :: jsonList
        }
        AddtoEs.addToEs(jsonList)
      } catch {
        case e: Exception => println("Exception: "+ e.getMessage)
      } finally {
        mdbClient.close()
      }
    }
  }

  def getCollection(_db: String, _collection: String, mongoClient: MongoClient): (MongoCollection, MongoClient) = {
    val db = mongoClient(_db)
    val collection = db(_collection)
    (collection, mongoClient)
  }

  def getMongoClient(host: String, port: Int): MongoClient = {
    MongoClient(host, port)
  }
}
