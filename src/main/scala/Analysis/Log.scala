package Analysis


/**
 * Created by kenneththomas on 1/13/16.
 */
case class Log(domain: String, logType: String, timeStamp: String, timeMilli: Long,
               ip: String, username: String, week: Int, year: Int)

object Log{
  def apply(domain: String, log_type: String, ip: String, username: String): Log = {
    Log(domain, log_type, "", 0, ip, username, 0, 0)
  }
}