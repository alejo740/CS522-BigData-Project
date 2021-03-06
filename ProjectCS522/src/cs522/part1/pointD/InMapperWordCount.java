package cs522.part1.pointD;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class InMapperWordCount {

	public static class MapProcess extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Logger logger = Logger.getLogger(MapProcess.class);
		private Map<Text, Integer> wordMap;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("---MAP SETUP---");
			wordMap = new TreeMap<Text, Integer>();
		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			logger.info("---MAP PROCESS---");
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				Text word = new Text();
				word.set(token);
				if (!wordMap.containsKey(word)) {
					wordMap.put(word, 1);
				} else {
					wordMap.put(word, wordMap.get(word) + 1);
				}
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("---MAP CLOSE---");
			for (Entry<Text, Integer> entry : wordMap.entrySet()) {
				context.write(entry.getKey(), new IntWritable(entry.getValue()));
			}
		}
	}

	public static class ReduceProcess extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private Logger logger = Logger.getLogger(ReduceProcess.class);
		@Override
		protected void setup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("---REDUCE SETUP---");
			super.setup(context);
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			logger.info("---REDUCE PROCESS---");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			logger.info("---REDUCE CLOSE---");
			super.cleanup(context);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "inmapperwordcount");
		job.setJarByClass(InMapperWordCount.class);

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