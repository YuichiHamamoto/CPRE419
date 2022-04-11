import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

public class Exp2 {

  //private static final int numOfReducer = 10;

  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage <input> <output>");
      System.exit(1);
    }
    SparkConf sparkConf = new SparkConf().setAppName("Lab6 exp2");
    //.setMaster("local[*]");
    JavaSparkContext context = new JavaSparkContext(sparkConf);

    //read edges and remove empty line
    JavaRDD<String> file = context.textFile(args[0]);
    JavaRDD<String> cleaned = file.filter(
      new Function<String, Boolean>() {
        public Boolean call(String s) {
          return !s.isEmpty();
        }
      }
    );

    //Store edges with both way because this is undirected graph
    JavaPairRDD<Long, Long> edges = cleaned.flatMapToPair(
      new PairFlatMapFunction<String, Long, Long>() {
        @SuppressWarnings("unchecked")
        public Iterator<Tuple2<Long, Long>> call(String s) {
          String[] st = s.trim().replaceAll("\\s+", " ").split(" ");
          return Arrays
            .asList(
              new Tuple2<Long, Long>(
                Long.parseLong(st[0]),
                Long.parseLong(st[1])
              ),
              new Tuple2<Long, Long>(
                Long.parseLong(st[1]),
                Long.parseLong(st[0])
              )
            )
            .iterator();
        }
      }
    );

    //group edge by start node
    JavaPairRDD<Long, Iterable<Long>> tri = edges.groupByKey();

    //populate the record with (key, value)= (the neighbor of the key, input tuple)
    JavaPairRDD<Long, Tuple2<Long, Iterable<Long>>> temp = tri.flatMapToPair(
      new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, Tuple2<Long, Iterable<Long>>>() {
        public Iterator<Tuple2<Long, Tuple2<Long, Iterable<Long>>>> call(
          Tuple2<Long, Iterable<Long>> s
        ) {
          Iterable<Long> values = s._2;
          List<Tuple2<Long, Tuple2<Long, Iterable<Long>>>> output = new ArrayList<Tuple2<Long, Tuple2<Long, Iterable<Long>>>>();

          for (Long value : values) {
            output.add(
              new Tuple2<Long, Tuple2<Long, Iterable<Long>>>(value, s)
            );
          }

          return output.iterator();
        }
      }
    );

    //group them by key
    JavaPairRDD<Long, Iterable<Tuple2<Long, Iterable<Long>>>> grouped = temp.groupByKey();

    //find the triangle
    JavaRDD<Tuple3<Long, Long, Long>> temp2 = grouped.flatMap(
      new FlatMapFunction<Tuple2<Long, Iterable<Tuple2<Long, Iterable<Long>>>>, Tuple3<Long, Long, Long>>() {
        public Iterator<Tuple3<Long, Long, Long>> call(
          Tuple2<Long, Iterable<Tuple2<Long, Iterable<Long>>>> s
        ) {
          //key of the input
          long key = s._1;
          //value of the input
          Iterable<Tuple2<Long, Iterable<Long>>> values = s._2;
          //hashSet to store which node is reachable
          HashSet<Long> set = new HashSet<Long>();
          for (Tuple2<Long, Iterable<Long>> value : values) {
            set.add(value._1);
          }

          //add possible triangle
          List<Tuple3<Long, Long, Long>> output = new ArrayList<Tuple3<Long, Long, Long>>();
          for (Tuple2<Long, Iterable<Long>> value : values) {
            for (Long nei : value._2) {
              if (set.contains(nei)) {
                Long[] tri = { key, value._1, nei };
                //sort the nodes in order to delete the duplicate later
                Arrays.sort(tri);
                output.add(
                  new Tuple3<Long, Long, Long>(tri[0], tri[1], tri[2])
                );
              }
            }
          }
          return output.iterator();
        }
      }
    );

    //eliminate duplicate triangles
    JavaRDD<Tuple3<Long, Long, Long>> temp3 = temp2.distinct();

    //Store the number of triangle
    List<Long> list = new ArrayList<Long>();
    list.add(temp3.count());
    JavaRDD<Long> result = context.parallelize(list);

    //save
    result.saveAsTextFile(args[1]);
    context.stop();
    context.close();
  }
}
