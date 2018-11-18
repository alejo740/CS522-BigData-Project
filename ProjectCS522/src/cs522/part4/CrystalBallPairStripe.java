package cs522.part4;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class CrystalBallPairStripe {

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
					j++;
				}
			}
		}
	}

	public static class ReduceProcess extends
			Reducer<Pair, IntWritable, Text, Text> {
		private Logger logger = Logger.getLogger(ReduceProcess.class);
		private String lastKey;
		private MapWritable mapData;

		@Override
		protected void setup(
				Reducer<Pair, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			logger.info("---REDUCE SETUP---");
			mapData = new MapWritable();
			lastKey = "";
		}

		public void reduce(Pair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			logger.info("---REDUCE PROCESS---");
			if (!key.getKey().equals(lastKey) && !lastKey.isEmpty()) {
				emitResult(context);
			}
			float total = 0;
			for (IntWritable val : values) {
				total += val.get();
			}
			mapData.put(new Text(key.getValue()), new FloatWritable(total));
			lastKey = key.getKey();
		}

		@Override
		protected void cleanup(
				Reducer<Pair, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			logger.info("---REDUCE CLOSE---");
			emitResult(context);
		}
		
		private void emitResult(Context context) throws IOException, InterruptedException{
			StringBuilder mapDataString = new StringBuilder();
			float sum = 0;
			for (Entry<Writable, Writable> entry : mapData.entrySet()) {
				sum += ((FloatWritable)entry.getValue()).get();
			}
			
			for (Entry<Writable, Writable> entry : mapData.entrySet()) {
				float c = ((FloatWritable)entry.getValue()).get() / sum;
				mapData.put(entry.getKey(), new FloatWritable(c));
				mapDataString.append("|").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("| ");
			}
			context.write(new Text(lastKey), new Text(mapDataString.toString().trim()));
			mapData = new MapWritable();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "CrystalBallPairStripe");
		job.setJarByClass(CrystalBallPairStripe.class);

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
