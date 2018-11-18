package cs522.part2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.log4j.Logger;

public class CrystalBallPair {

	public static final String COUNTER_TOKEN = "*";

	public static class MapProcess extends
			Mapper<LongWritable, Text, Pair, IntWritable> {
		private Logger logger = Logger.getLogger(MapProcess.class);
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			logger.info("---MAP PROCESS---");
			String line = value.toString().trim();
			String[] chunks = line.split("\\s+");

			for (int i = 0; i < chunks.length; i++) {
				int j = i + 1;
				while (j < chunks.length && !chunks[i].equals(chunks[j])) {
					context.write(new Pair(chunks[i], chunks[j]), new IntWritable(1));
					context.write(new Pair(chunks[i], COUNTER_TOKEN), new IntWritable(1));
					j++;
				}
			}
		}
	}

	public static class ReduceProcess extends
			Reducer<Pair, IntWritable, Text, FloatWritable> {
		private Logger logger = Logger.getLogger(ReduceProcess.class);
		private float count;

		@Override
		protected void setup(
				Reducer<Pair, IntWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("---REDUCE SETUP---");
			count = 0;
		}

		public void reduce(Pair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			logger.info("---REDUCE PROCESS---");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			if (key.getValue().equals(COUNTER_TOKEN)) {
				count = sum;
			} else {
				context.write(new Text(key.toString()), new FloatWritable(sum / count));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "CrystalBallPair");
		job.setJarByClass(CrystalBallPair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(Pair.class);

		job.setMapperClass(MapProcess.class);
		job.setReducerClass(ReduceProcess.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
