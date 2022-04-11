
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.io.IOException;

@SuppressWarnings("serial")
public class FindingSum1 {
        
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
              .flatMap(new Tokenizer());

        // emit result
        counts.addSink(new CustomSinkFunction());
 
        env.execute("Streaming Sum Aggregation Example");
    }
    
    public static final class CustomSinkFunction extends RichSinkFunction<Integer> {

        int total_sum = 0;

        // this function is called per input
        public void invoke(Integer value) throws Exception {
            total_sum += value;
        }

        @Override
        public void close() throws IOException { 
            System.out.println("----------------------------------------");
            System.out.println("TOTAL SUM: " + String.valueOf(total_sum));
        }
 
    }

    public static final class Tokenizer implements FlatMapFunction<String, Integer> {
		
        public void flatMap(String value, Collector<Integer> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(Integer.valueOf(token));
                }
            }
        }
    }

}

