/**
 * Created by kenneththomas on 11/30/15.
 */

import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.util.Locale
import scala.util.control.Exception._
import java.util.regex.Matcher
import scala.util.{Try, Success, Failure}


@SerialVersionUID(100L)
class AccessLogParser extends Serializable {

  private val regex = """(([0-9.]+)|(\D*\.\w+)|(.*[0-9.]+\D*[\-|\.].*\.\w+))\s(.*|\-)\s(\w*|\-)\s\[(.*)\]\s\"(.*)\s(\/.*)*\s(HTTP\/.*[^”]+)\"\s(\d{3})\s(\d+|-)($|\s\"((?:[^”])+)\"\s\"((?:[^”]|”)+)\"$)"""
  private val p = Pattern.compile(regex)

    def parseRecord(record: String): Option[AccessLogRecord] = {
    val matcher = p.matcher(record)
      matcher.find match {
        case true =>
          Some(buildAccessLogRecord(matcher))
        case _ => None
      }
  }


  def parseRecordReturningNullObjectOnFailure(record: String): AccessLogRecord = {
    val matcher = p.matcher(record)
    if (matcher.find) {
      buildAccessLogRecord(matcher)
    } else {
      AccessLogParser.nullObjectAccessLogRecord
    }
  }

  private def buildAccessLogRecord(matcher: Matcher) = {
    AccessLogRecord(
      matcher.group(1),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9),
      matcher.group(10),
      matcher.group(11),
      matcher.group(12),
      matcher.group(13),
      matcher.group(14),
      matcher.group(15))
  }
}

/**
 *
 * 64.242.88.10 - - [07/Mar/2004:22:03:19 -0800] "GET /twiki/bin/rdiff/Main/VishaalGolam HTTP/1.1" 200 5055
 * 64.242.88.10 - - [07/Mar/2004:22:04:44 -0800] "GET /twiki/bin/view/Main/TWikiUsers?rev=1.21 HTTP/1.1" 200 6522
 * 64.242.88.10 - - [07/Mar/2004:22:06:16 -0800] "GET /twiki/bin/edit/Main/Delay_notice_recipient?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
 *
 */
object AccessLogParser {

  val nullObjectAccessLogRecord = AccessLogRecord("", "", "", "", "", "", "", "", "", "", "", "")

  def parseRequestField(request: String): Option[Tuple3[String, String, String]] = {
    val arr = request.split(" ")
    if (arr.size == 3) Some((arr(0), arr(1), arr(2))) else None
  }

  /**
   * @param field a String that looks like "[21/Jul/2009:02:48:13 -0700]"
   */
  def parseDateField(field: String): Option[java.util.Date] = {
    val dateRegex = "\\[(.*?) .+]"
    val datePattern = Pattern.compile(dateRegex)
    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      println("***** DATE STRING" + dateString)
      // HH is 0-23; kk is 1-24
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      allCatch.opt(dateFormat.parse(dateString))  // return Option[Date]
    } else {
      None
    }
  }

}

case class AccessLogRecord (
                             clientIpAddress: String,         // should be an ip address, but may also be the hostname if hostname-lookups are enabled
                             rfc1413ClientIdentity: String,   // typically `-`
                             remoteUser: String,              // typically `-`
                             dateTime: String,                // [day/month/year:hour:minute:second zone]
                             request: String,                 // `GET`
                             requestURL: String,              // `/foo ...`
                             httpVersion: String,             // `HTTP/1.1`
                             httpStatusCode: String,          // 200, 404, etc.
                             bytesSent: String,               // may be `-`
                             extraData: String,
                             referer: String,         // where the visitor came from
                             userAgent: String        // long string to represent the browser and OS
                             )
