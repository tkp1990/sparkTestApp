package Analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Created by kenneththomas on 1/13/16.
 */
case class ESConfig(esNode: String, esPort: String)

object ESConfig extends ((String, String) => ESConfig) {
  def getEsConfig(esNode: String, esPort: String): ESConfig = ESConfig(esNode, esPort)
}

object SparkConfig {

  def getSparkConfig(esConf: ESConfig): SparkConf = {
    new SparkConf().setMaster(Constants.SPARK_MASTER)
      .setAppName(Constants.SPARK_APP_NAME)
      .set(Constants.ES_NODE, esConf.esNode)
      .set(Constants.ES_PORT, esConf.esPort)
  }

  def getSparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }

  def getSqlContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }
}
