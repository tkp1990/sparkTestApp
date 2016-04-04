import org.json.JSONObject
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

type innerMap = mutable.HashMap[String, Any]
type mp = mutable.Map[String, Any]
var x:Map[String,Map[String,String]] = Map()
x += ("x" -> Map("x1" -> "x2"))
var y:Map[String,Map[String,String]] = Map()
y += ("x" -> Map("y1" -> "y2"))
x.get("x") match {
  case Some(xx) =>
    val z:Map[String,String] = xx + ("fdfdfd" -> "fdfd")
    x.update("x",z)
  case _ =>
}
x
var set = new mutable.ArrayStack[String]()
set += "root"
set += "bin"
//var m = Map("root" -> Map("twiki" -> Map("bin" -> Map("edit" -> Map("Main" -> Map("Double_bounce_sender" -> Map()))))))
var list:List[String] = List("root", "bin", "etc", "twiki", "some")
//list ++ List("ken")
list + "ken"
list
val b = list.zipWithIndex.map(_.swap).toMap
var m1: Map[String, Int] = Map()
val m2: Map[String, Int] = Map()
for(a <- b.keySet){
  println(a)
  m1 += (b.get(a).get -> a)
  m2 ++= m1
}
m1
m2

var list1:List[String] = List("1980-04-01", "33")
var list3 = List("abcd", "ads")
list1 ++ list3

var dateListJson:List[JSONObject] = List()[JSONObject]
va log = new JSONObject()
log.put("a", "v")
dateListJson + log.toString
dateListJson.foreach(println)