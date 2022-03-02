import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import javax.lang.model.element.ElementVisitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class customSort {

  public static void main(String[] args) throws Exception {
    String input = "/lab3/data/input-500k";
    String temp = "/user/hamamoto/lab3/exp2/temp";
    String output = "/user/hamamoto/lab3/output/exp2";
    ///////////////////////////////////////////////////
    ///////////// First Round MapReduce ///////////////
    ////// where you might want to do some sampling ///
    ///////////////////////////////////////////////////
    int reduceNumber = 10;

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Exp2");

    job.setJarByClass(customSort.class);
    job.setNumReduceTasks(reduceNumber);

    job.setMapperClass(mapOne.class);
    //job.setCombinerClass(combinerOne.class);
    job.setReducerClass(reduceOne.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(input));

    FileOutputFormat.setOutputPath(job, new Path(temp));

    job.waitForCompletion(true);

    ///////////////////////////////////////////////////
    ///////////// Second Round MapReduce //////////////
    ///////////////////////////////////////////////////
    Job job_two = Job.getInstance(conf, "Round Two");
    job_two.setJarByClass(customSort.class);

    conf.setInt("Count", 0);
    // Providing the number of reducers for the second round
    reduceNumber = 10;
    job_two.setNumReduceTasks(reduceNumber);

    // Should be match with the output datatype of mapper and reducer
    job_two.setMapOutputKeyClass(Text.class);
    job_two.setMapOutputValueClass(Text.class);

    job_two.setOutputKeyClass(Text.class);
    job_two.setOutputValueClass(Text.class);

    job_two.setMapperClass(mapTwo.class);
    job_two.setReducerClass(reduceTwo.class);

    // Partitioner is our custom partitioner class
    job_two.setPartitionerClass(MyPartitioner.class);

    // Input and output format class
    job_two.setInputFormatClass(KeyValueTextInputFormat.class);
    job_two.setOutputFormatClass(TextOutputFormat.class);

    // The output of previous job set as input of the next
    FileInputFormat.addInputPath(job_two, new Path(temp));
    FileOutputFormat.setOutputPath(job_two, new Path(output));

    // Run the job
    System.exit(job_two.waitForCompletion(true) ? 0 : 1);
  }

  public static class mapOne extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class reduceOne extends Reducer<Text, Text, Text, Text> {

    private Text line = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text val : values) {
        //doulbe check the format of the input file
        if (key.toString().length() != 15 && val.toString().length() != 40) {
          return;
        }
        context.write(key, val);
      }
    }
  }

  // Compare each input key with the boundaries we get from the first round
  // And add the partitioner information in the end of values
  public static class mapTwo extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
      // For instance, partitionFile0 to partitionFile8 are the discovered boundaries,
      // based on which you add the No ID 0 to 9 at the end of value
      // How to find the boundaries is your job for this experiment
      String s = key.toString();
      String val = value.toString();
      //doulbe check the format of the input file
      if (s.length() != 15 && val.length() != 40) {
        return;
      }

      //partite into 10
      if (s.compareTo("A") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(0))
        );
      } else if (s.compareTo("E") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(1))
        );
      } else if (s.compareTo("I") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(2))
        );
      } else if (s.compareTo("N ") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(3))
        );
      } else if (s.compareTo("S") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(4))
        );
      } else if (s.compareTo("X") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(5))
        );
      } else if (s.compareTo("c") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(6))
        );
      } else if (s.compareTo("h") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(7))
        );
      } else if (s.compareTo("m") <= 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(8))
        );
      } else if (s.compareTo("m") > 0) {
        context.write(
          new Text(s),
          new Text(value.toString() + ";" + Integer.toString(9))
        );
      }
    }
  }

  public static class reduceTwo extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text val : values) {
        String[] tmp = val.toString().split(";");
        context.write(key, new Text(tmp[0]));
      }
    }
  }

  // Extract the partitioner information from the input values, which decides the destination of data
  public static class MyPartitioner extends Partitioner<Text, Text> {

    public int getPartition(Text key, Text value, int numReduceTasks) {
      String[] desTmp = value.toString().split(";");
      return Integer.parseInt(desTmp[1]);
    }
  }
}
