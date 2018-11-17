package cs522.part1.pointE;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class AverageAlgorithm {
	public static class MapProcess extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] chunks = line.split(" ");
			String id = chunks[0];			
			String lastChunk = chunks[chunks.length-1];			
			Integer number = 0;
			try {
				number = Integer.parseInt(lastChunk);
			} catch (NumberFormatException e) {

			}
			
			context.write(new Text(id), new IntWritable(number));
		}
	}

	public static class ReduceProcess extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new IntWritable(sum/count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "AverageAlgorithm");
		job.setJarByClass(AverageAlgorithm.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MapProcess.class);
		job.setReducerClass(ReduceProcess.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
