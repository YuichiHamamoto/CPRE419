import java.util.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class Bigram {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    // get input data
    // <PATH_TO_DATA>: The path to input data, e.g., "/home/cpre419/Downloads/shakespeare"
    DataStream<String> text = env.readTextFile(
      "/home/cpre419/Downloads/shakespeare"
    );

    DataStream<Tuple2<String, Integer>> counts =
      // split up the lines in pairs (2-tuples) containing: (word,1)
      text
        .flatMap(new Tokenizer())
        // group by the tuple field "0" and sum up tuple field "1"
        .keyBy(0)
        .sum(1);

    // emit result
    //counts.print();

    HashMap<String, Integer> map = new HashMap<String, Integer>();
    Iterator<Tuple2<String, Integer>> output = DataStreamUtils.collect(counts);
    while (output.hasNext()) {
      Tuple2<String, Integer> temp = output.next();
      map.put(temp.f0, temp.f1);
    }

    // Create a list from elements of HashMap
    List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(
      map.entrySet()
    );

    // Sort the list
    Collections.sort(
      list,
      new Comparator<Map.Entry<String, Integer>>() {
        public int compare(
          Map.Entry<String, Integer> o1,
          Map.Entry<String, Integer> o2
        ) {
          return (o1.getValue()).compareTo(o2.getValue());
        }
      }
    );

    for (int i = list.size() - 1; i >= list.size() - 11; i--) {
      System.out.println(
        "(" + list.get(i).getKey() + "," + list.get(i).getValue() + ")"
      );
    }
    //env.execute("Streaming WordCount");
  }

  public static final class Tokenizer
    implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      // normalize and split the line
      value = value.toLowerCase();
      value = value.replaceAll("[^a-z0-9]", " ");
      value = value.replaceAll("\\s+", " ");
      String words[] = value.split(" ");
      String pre = null;
      for (String word : words) {
        if (
          pre == null ||
          pre.equals(".") ||
          word.equals(".") ||
          pre.equals("") ||
          word.equals("")
        ) {} else {
          String token = pre + " " + word;
          if (token.length() > 0) {
            out.collect(new Tuple2<String, Integer>(token, 1));
          }
        }
        pre = word;
      }
    }
  }
}
