
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

@SuppressWarnings("serial")
public class FindingSum2 {
        
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = env.readTextFile(<PATH_TO_DATA>);

        // this code is performed per input line
        DataStream<Integer> counts =
              text
              .countWindowAll(100)
              .apply(new AllWindowFunction<String, Integer, GlobalWindow>() {  
                  public void apply(GlobalWindow window, Iterable<String> values, 
                          Collector<Integer> out) throws Exception {
                      
                      int total_sum = 0;
                      
                      for (String value : values) {
                          total_sum += Integer.valueOf(value);
                      }
                      
                      out.collect(total_sum);
                  }
              });

        // emit result
        counts.print();
 
        env.execute("Streaming Sum Aggregation Example");
    }
}