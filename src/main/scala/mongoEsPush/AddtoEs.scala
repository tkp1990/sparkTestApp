package mongoEsPush

import org.apache.spark.{SparkContext, SparkConf}
import play.api.libs.json.{Json, JsObject, JsValue}
import org.elasticsearch.spark._
/**
 * Created by kenneththomas on 4/4/16.
 */
object AddtoEs {


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


}
