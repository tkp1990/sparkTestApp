import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.feature.{Word2Vec, IDF, HashingTF}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by kenneththomas on 12/11/15.
 */
object RottenTomatoes {
  val url = """src/main/resources/RottenTomatoe/"""
  def main(args: Array[String]) {
    val conf = new org.apache.spark.SparkConf()
      .setMaster("local[*]")
      .setAppName("RottenTomatoesTest")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.executor.memory", "3096m")
      .set("spark.executor.cores", "4")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max.mb", "256")

    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val data = sc.textFile(url+"train.tsv")
    val test = sc.textFile(url+"test.tsv")
    println("Class Type", data.getClass )
    val reviews = data.map(_.split("\t"))
    println("Count: ", reviews.count())
    val testData = test.map(_.split("\t"))
    val wordCounts = reviews.flatMap(x => x(2).split(" ")).map((_, 1)).reduceByKey((a, b) => a + b).sortBy(_._2, false)
    //wordCounts.take(5).foreach(println)

    /**
     * tf-idf(term frequency - inverse document frequency)
     * tf-idf = tf * idf
     * [term frequency - number of times a term appears in a Document.
     *  inverse document frequency - number of documents the term appears in.]
     */

    val document: RDD[Seq[String]] = data.map(_.split("\t").toSeq)
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(document)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    println("------------------TF-IDF-----------------")
    tfidf.take(10).foreach(println)

    /**
     * Naive Bayes Classification
     * Creating a LabeledPointRDD to train the NaiveBayes Algo and then use it to predict on the test data
     */

    val htf = new HashingTF(10000)
    //Converting training data to LabeledRDD
    val trueReviews = reviews.filter(x => !x(0).contains("PhraseId"))
    val parsedData = trueReviews.map{
      line => LabeledPoint(line(0).toDouble, htf.transform(line(2).split(" ")))
    }
    //Converting test data to LabeledRDD
    val testReviews = testData.filter(x => !x(0).contains("PhraseId")).map{
      line => LabeledPoint(line(0).toDouble, htf.transform(line(2).split(' ')))
    }
    val model = NaiveBayes.train(parsedData);
    val predictionAndLabel = testReviews.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("---------------Prediction Values for test data!!---------------")
    predictionAndLabel.take(10).foreach(println)

    /**
     * Converting RDD to a DataFrame
     * Creating a Table Structure, which can be queried using SQL or Sparks query language
     */

    val a: RDD[RottenTomatoes.review] = reviews.filter(x => !x(0).contains("PhraseId")).map(x => review(x(0).toInt, x(1).toInt, x(2), x(3).toInt))
    //a.foreach(println)
    val df: DataFrame = sqlContext.createDataFrame(a: RDD[RottenTomatoes.review])
    df.registerTempTable("df")

    sqlContext.sql("""
  SELECT pId,
         sId,
         phrase,
         sentiment
  FROM df""").show

  }

  case class review(pId: Integer, sId: Integer, phrase: String, sentiment: Integer)

}
