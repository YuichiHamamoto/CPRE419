import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class WordCountFlink {

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
    counts.print();

    env.execute("Streaming WordCount Example");
  }

  public static final class Tokenizer
    implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      // normalize and split the line
      String[] tokens = value.toLowerCase().split("\\W+");

      // emit the pairs
      for (String token : tokens) {
        if (token.length() > 0) {
          out.collect(new Tuple2<String, Integer>(token, 1));
        }
      }
    }
  }
}
