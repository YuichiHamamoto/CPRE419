import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class WordCountSpark {

  private static final int numOfReducers = 2;

  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input> <output>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("WordCount in Spark");
    //.setMaster("local[*]");
    JavaSparkContext context = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = context.textFile(args[0]);

    JavaRDD<String> words = lines.flatMap(
      new FlatMapFunction<String, String>() {
        public Iterator<String> call(String s) {
          return Arrays.asList(s.split("\\s+")).iterator();
        }
      }
    );

    // JavaPairRDD<key_type, value_type> var2 = var1.mapToPair(new PairFunction<input_type, key_type, value_type>() {
    //   public Tuple2<key_type, value_type> call(input_type s) {
    //	   ......
    //	   return new Tuple2<key_type, value_type>(..., ...);
    //   }
    // });
    JavaPairRDD<String, Integer> ones = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }
    );

    // JavaPairRDD<key_type, output_type> var2 = var1.reduceByKey(new Function2<value_type, value_type, output_type>() {
    //   public output_type call(value_type i1, value_type i2) {
    //	   return ......
    //   }
    // }, numOfReducers);
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      },
      numOfReducers
    );

    //swap key and value to sort by key
    JavaPairRDD<Integer, String> swapped = counts.mapToPair(
      new PairFunction<Tuple2<String, Integer>, Integer, String>() {
        public Tuple2<Integer, String> call(Tuple2<String, Integer> item)
          throws Exception {
          return item.swap();
        }
      }
    );

    //sort
    JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);

    sorted.saveAsTextFile(args[1]);
    context.stop();
    context.close();
  }
}
