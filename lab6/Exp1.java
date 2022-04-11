import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Exp1 {

  //private static final int numOfReducers = 2;

  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: <input> <output>");
      System.exit(1);
    }
    SparkConf sparkConf = new SparkConf().setAppName("Lab5 exp1");
    //.setMaster("local[*]");
    JavaSparkContext context = new JavaSparkContext(sparkConf);

    //githuv.csv
    JavaRDD<String> file = context.textFile(args[0]);

    //map by (key, value)=(language, line)
    JavaPairRDD<String, String> git = file.mapToPair(
      new PairFunction<String, String, String>() {
        public Tuple2<String, String> call(String s) {
          String[] st = s.split(",");
          return new Tuple2<String, String>(st[1], s);
        }
      }
    );

    //group by language
    JavaPairRDD<String, Iterable<String>> grouped = git.groupByKey();

    //for each language return the info with given format
    JavaPairRDD<Long, String> output = grouped.mapToPair(
      new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
        public Tuple2<Long, String> call(
          Tuple2<String, Iterable<String>> values
        ) {
          //# of record
          long count = 0;
          //max star
          long max = 0;
          //name of repo holds the max
          String repo = "";
          //name of language
          String lang = "";
          for (String s : values._2) {
            String[] st = s.split(",");
            count++;
            if (Long.valueOf(st[12]) > max) {
              max = Long.valueOf(st[12]);
              repo = st[0];
              lang = st[1];
            }
          }
          return new Tuple2<Long, String>(
            count,
            lang + " " + count + " " + repo + " " + max
          );
        }
      }
    );

    //sort
    JavaPairRDD<Long, String> sorted = output.sortByKey(false);

    //save
    sorted.values().saveAsTextFile(args[1]);
    context.stop();
    context.close();
  }
}
