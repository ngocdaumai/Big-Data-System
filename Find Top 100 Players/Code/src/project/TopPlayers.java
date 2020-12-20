package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;

import static java.lang.System.out;

class PlayingRecord implements Writable {
  Text userId;
  LongWritable duration; 
  LongWritable score;

  public PlayingRecord() {
    this.userId = new Text();
    this.duration = new LongWritable(); 
    this.score = new LongWritable();
  }

  public PlayingRecord(String userId, Long duration, Long score) {
    this.userId = new Text(userId);
    this.duration = new LongWritable(duration);
    this.score = new LongWritable(score);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    userId.write(out);
    duration.write(out);
    score.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    userId.readFields(in);
    duration.readFields(in);
    score.readFields(in);
  }
}

class DescDoubleComparator extends WritableComparator {
  public DescDoubleComparator() {
    super(DoubleWritable.class);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    double thisValue = readDouble(b1, s1);
    double thatValue = readDouble(b2, s2);
    return - Double.compare(thisValue, thatValue);
  }
}


public class TopPlayers extends Configured implements Tool {

	  public static void main(String[] args) throws Exception {
	    int exit_code = ToolRunner.run(new TopPlayers(), args);
	    System.exit(exit_code);
	  }

	  @Override
	  public int run(String[] args) throws Exception {
	    out.println("Args: ");
	    for (String arg : args) {
	      System.out.println(arg);
	    }
	    out.println("End Args.");

	    Configuration conf = new Configuration();
	    String[] remaining_args = new GenericOptionsParser(conf, args).getRemainingArgs();

	    
	    conf.set("hadoop.job.history.user.location", "none");

	    String inputPath = remaining_args[0];
	    String outputPath = remaining_args[2];
	    String tempPath = remaining_args[1];

	    Job job = Job.getInstance(conf, "aggregation");
	    job.setJarByClass(getClass());
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(tempPath));
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

	    job.setMapperClass(InputMapper.class);
	    job.setReducerClass(TopValuesReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PlayingRecord.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.waitForCompletion(true);

	    Job job2 = Job.getInstance(conf, "sorting");
	    job2.setJarByClass(getClass());
	    FileInputFormat.setInputPaths(job2, new Path(tempPath));
	    FileOutputFormat.setOutputPath(job2, new Path(outputPath));
	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);

	    job2.setNumReduceTasks(1);
	    job2.setMapperClass(InverseMapper.class);
	    job2.setMapOutputKeyClass(DoubleWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setSortComparatorClass(DescDoubleComparator.class);
	    job2.setReducerClass(Sort.class);

	    return job2.waitForCompletion(true) ? 0 : 1;
	  }

	  public static class Sort extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	    private int count = 0;
	    @Override
	    public void setup(Context context) {
	    }

	    @Override
	    protected void reduce(DoubleWritable score, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
	      if (count < 100) {
	        for (Text userId : values) {
	          context.write(userId, score);
	          count += 1;
	          if (count >= 100) break;
	        }
	      }
	    }
	  }

	  public static class InputMapper extends Mapper<LongWritable, Text, Text, PlayingRecord> {

	    @Override
	    public void setup(Context context) {
	    }

	    @Override
	    protected void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
	      String[] columns = value.toString().split(",");
	      Text k = new Text(columns[0]);
	      long duration =
	          Duration.between(LocalTime.parse(columns[1]), LocalTime.parse(columns[2])).getSeconds();
	      PlayingRecord r = new PlayingRecord(columns[0], duration, Long.parseLong(columns[3]));
	      context.write(k, r);
	    }
	  }

	  public static class TopValuesReducer extends Reducer<Text, PlayingRecord, Text, DoubleWritable> {

	    @Override
	    public void setup(Context context) {
	    }

	    @Override
	    protected void reduce(Text key, Iterable<PlayingRecord> values, Context context)
	        throws IOException, InterruptedException {
	      long sumScore = 0;
	      long totalDuration = 0;
	      for (PlayingRecord val : values) {
	        sumScore += val.score.get();
	        totalDuration += val.duration.get();
	      }
	      double avgScore = sumScore / (totalDuration / 60.0); // avgScore per minute
	      context.write(key, new DoubleWritable(avgScore));
	    }
	  }
	}

