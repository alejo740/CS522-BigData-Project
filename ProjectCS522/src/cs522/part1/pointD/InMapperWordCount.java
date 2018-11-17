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

public class InMapperWordCount {

	public static class MapProcess extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Map<Text, Integer> wordMap;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("Mapper Initialize");
			wordMap = new TreeMap<Text, Integer>();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();

				Text word = new Text();
				word.set(token);

				System.out.println(token);
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
			System.out.println("----Mapper Close----");

			for (Entry<Text, Integer> entry : wordMap.entrySet()) {
				context.write(entry.getKey(), new IntWritable(entry.getValue()));
			}

		}

	}

	public static class ReduceProcess extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void setup(
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("----Reducer Initialize----");
			super.setup(context);
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
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