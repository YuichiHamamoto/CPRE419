import java.io.*;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class sorting {

  public static void main(String[] args) throws Exception {
    // Change following paths accordingly
    String input = "/lab3/data/input-5m";
    String temp = "/user/hamamoto/lab3/exp1/temp";
    String output = "/user/hamamoto/lab3/output/exp1";

    // The number of reduce tasks
    int reduce_tasks = 10;

    Configuration conf = new Configuration();

    // Create job for round 1
    Job job_one = Job.getInstance(conf, "Sorting Program Round One");

    // Attach the job to this Driver
    job_one.setJarByClass(sorting.class);

    // Fix the number of reduce tasks to run
    // If not provided, the system decides on its own
    // For the first MR round, we just need to convert data into sequence file format
    // As a result, we don't need any reduce task. However, In this lab we don't
    // need to do this, because we are looking for ASCII sort and the SequenceFileFormat
    // is neccesary for integer sorting.
    job_one.setNumReduceTasks(0);

    // The datatype of the mapper output Key, Value
    job_one.setMapOutputKeyClass(Text.class);
    job_one.setMapOutputValueClass(Text.class);

    // The datatype of the reducer output Key, Value
    job_one.setOutputKeyClass(Text.class);
    job_one.setOutputValueClass(Text.class);

    // The class that provides the map method
    job_one.setMapperClass(Map_One.class);

    // Decides how the input will be split
    // We are using TextInputFormat which splits the data line by line
    // This means each map method receives one line as an input
    job_one.setInputFormatClass(TextInputFormat.class);

    // Decides the Output Format
    // We have set it to SequenceFile OutputFormat
    job_one.setOutputFormatClass(SequenceFileOutputFormat.class);

    // The input HDFS path for this job
    // The path can be a directory containing several files
    // You can add multiple input paths including multiple directories
    FileInputFormat.addInputPath(job_one, new Path(input));

    // This is legal
    // FileInputFormat.addInputPath(job_one, new Path(another_input_path));

    // The output HDFS path for this job
    // The output path must be one and only one
    // This must not be shared with other running jobs in the system
    FileOutputFormat.setOutputPath(job_one, new Path(temp));

    // This is not allowed
    // FileOutputFormat.setOutputPath(job_one, new Path(another_output_path));

    // Run the job
    job_one.waitForCompletion(true);

    // Create job for round 2
    // The output of the previous job can be passed as the input to the next
    // The steps are as in job 1

    Job job_two = Job.getInstance(conf, "Sorting Program Round Two");
    job_two.setJarByClass(sorting.class);
    job_two.setNumReduceTasks(reduce_tasks);

    // Should be match with the output datatype of mapper and reducer
    job_two.setMapOutputKeyClass(Text.class);
    job_two.setMapOutputValueClass(Text.class);
    job_two.setOutputKeyClass(Text.class);
    job_two.setOutputValueClass(Text.class);

    // The output of previous job set as input of the next
    FileInputFormat.addInputPath(job_two, new Path(temp));
    FileOutputFormat.setOutputPath(job_two, new Path(output));

    job_two.setInputFormatClass(SequenceFileInputFormat.class);
    job_two.setOutputFormatClass(TextOutputFormat.class);

    // RandomSampler is used, which chooses keys with a uniform probability
    // There are also parameters for the maximum number of samples to take and the maximum
    // number of splits to sample
    job_two.setPartitionerClass(TotalOrderPartitioner.class);
    InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
      0.01,
      1000,
      10
    );
    InputSampler.writePartitionFile(job_two, sampler);
    Configuration conf1 = job_two.getConfiguration();
    String partitionFile = TotalOrderPartitioner.getPartitionFile(conf1);
    URI partitionUri = new URI(partitionFile);
    job_two.addCacheFile(partitionUri);

    job_two.setMapperClass(Map_Two.class);
    job_two.setReducerClass(Reduce_Two.class);

    job_two.waitForCompletion(true);

    System.exit(job_two.waitForCompletion(true) ? 0 : 1);
  }

  // The Map Class
  public static class Map_One extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      // The TextInputFormat splits the data line by line.
      // So each map method receives one line from the input
      // Implement your functionality to split input into key and value
      String[] s = value.toString().split("\t");
      k.set(s[0]);
      v.set(s[1]);
      context.write(k, v);
    }
  }

  // The second Map Class
  public static class Map_Two extends Mapper<Object, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      // Split the value into key and value
      String[] s = value.toString().split("\t");
      k.set(s[0]);
      v.set(s[1]);
      context.write(k, v);
    }
  }

  // The second Reduce class
  // In the second reduce function, we already have sorted values in each reducer and because of having
  // the sampler partitioner we have balanced output files which files are also sorted such that R1 contains
  // smallest and all data in R2 are smaller than R3 and so on. Therefore, I just emit the obtained (key,
  // value) pairs with no additional work

  public static class Reduce_Two extends Reducer<Text, Text, Text, Text> {

    private Text word = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text val : values) {
        word.set(val.toString());
        context.write(key, word);
      }
    }
  }
}
