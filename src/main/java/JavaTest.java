import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;
import scala.Tuple2;

import java.util.List;

/**
 * Created by kenneththomas on 11/26/15.
 */
public class JavaTest {

    public static void main(String[] argv){
        //System.setProperty("hadoop.home.dir","/usr/local/hadoop");
        SparkConf conf = new SparkConf().setAppName("MySparkTest").setMaster("local");

        conf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set("es.nodes","localhost:9200");
        hadoopConfiguration.set("es.resource","bank/account");

        JavaPairRDD<Text,MapWritable> esRDD = sc.newAPIHadoopRDD(hadoopConfiguration, EsInputFormat.class, Text.class, MapWritable.class);
        System.out.println("Some Damn Data: "+esRDD.toString());
        Tuple2<Text, MapWritable> list = esRDD.first();
        System.out.println("The bloody list: "+list);
        System.out.println("Count of records founds is " + esRDD.count());

        //This function will get ES record key as first parameter and the ES record as second parameter, it will return {city,1} tuple for each city in the record
        JavaPairRDD<String, Integer> cityCountMap = esRDD.mapToPair(new PairFunction<Tuple2<Text, MapWritable>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Text, MapWritable> currentEntry) throws Exception {
                MapWritable valueMap = currentEntry._2();
                WritableArrayWritable address =(WritableArrayWritable) valueMap.get(new Text("address"));
                MapWritable addressMap = (MapWritable)address.get()[0];
                Text city = (Text)addressMap.get(new Text("city"));
                return new Tuple2<String, Integer>(city.toString(),1);
            }
        });

        //This is reducer which will maintain running count of city vs count
        JavaPairRDD<String, Integer> cityCount = cityCountMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer first, Integer second) throws Exception {
                return first + second;
            }
        });

        cityCount.saveAsTextFile("file:///tmp/sparkes");


    }
}
