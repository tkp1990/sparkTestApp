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
    implicit val p = new AccessLogParser()
    implicit val sc = new org.apache.spark.SparkContext(conf)

    val accessLog = sc.textFile(url+"access_log")
    //accessLog.map(line => makeMap(p.parseRecord(line))).map()
    accessLog.foreach(println)

    println("-----------------------------------------------------------------")

    //accessLog.foreach(a => makeMap(a))
    val count = accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").count()
    println("Count: ", count)

//    accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").map(getRequest(_)).count
//    val recs = accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").map(getRequest(_))
//    val distRecs = accessLog.filter(line => getAccessCode(p.parseRecord(line)) == "401").map(getRequest(_)).distinct()
//    distRecs.foreach(println)

    println("No of requests made per Ip: ")
    val ips = accessLog.map(line => getIp(p.parseRecord(line))).map(ip => (ip, 1)).reduceByKey(_ + _)
    ips.foreach(println)

    println("No of request Types: ")
    val requests = accessLog.map(line => requestType(p.parseRecord(line))).map(req => (req, 1)).reduceByKey(_ + _)
    requests.foreach(println)

    println("No of Status Codes Grouped: ")
    val statusCodes = accessLog.map(line => getAccessCode(p.parseRecord(line))).map(code => (code, 1)).reduceByKey(_ + _)
    statusCodes.foreach(println)

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
    println(data)
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



}
