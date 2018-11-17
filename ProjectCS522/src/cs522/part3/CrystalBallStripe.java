package cs522.part3;

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

public class CrystalBallStripe {

	public static class MapProcess extends
			Mapper<LongWritable, Text, Text, MapWritable> {
		
		private Logger logger = Logger.getLogger(MapProcess.class);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			logger.info("---MAP PROCESS---");
			String line = value.toString().trim();
			String[] chunks = line.split("\\s+");

			for (int i = 0; i < chunks.length; i++) {
				int j = i + 1;
				MapWritable mapWindow = new MapWritable();
				while (j < chunks.length && !chunks[i].equals(chunks[j])) {
					Text tj = new Text(chunks[j]);
					float sum = 0;
					if (!mapWindow.containsKey(tj)) {
						sum = 1;
					} else {
						sum = ((FloatWritable) mapWindow.get(tj)).get() + 1;
					}
					mapWindow.put(tj, new FloatWritable(sum));
					j++;
				}

				context.write(new Text(chunks[i]), mapWindow);
			}
		}
	}

	public static class ReduceProcess extends
			Reducer<Text, MapWritable, Text, Text> {
		
		private Logger logger = Logger.getLogger(ReduceProcess.class);

		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			logger.info("---REDUCE PROCESS---");
			
			MapWritable mapResult = new MapWritable();
			for (MapWritable val : values) {
				for (Entry<Writable, Writable> entry : val.entrySet()) {
					float value = 0;
					if (!mapResult.containsKey(entry.getKey())) {
						value = ((FloatWritable) entry.getValue()).get();
					} else {
						value = ((FloatWritable) mapResult.get(entry.getKey()))
								.get()
								+ ((FloatWritable) entry.getValue()).get();
					}
					mapResult.put(entry.getKey(), new FloatWritable(value));
				}
			}

			float sum = 0;
			for (Entry<Writable, Writable> entry : mapResult.entrySet()) {
				sum += ((FloatWritable) entry.getValue()).get();
			}

			StringBuilder mapResultOutput = new StringBuilder();
			for (Entry<Writable, Writable> entry : mapResult.entrySet()) {
				float value = ((FloatWritable) entry.getValue()).get() / sum;
				mapResult.put(entry.getKey(), new FloatWritable(value));
				mapResultOutput.append("|").append(entry.getKey()).append("->").append(entry.getValue()).append("| ");
			}
			
			context.write(key, new Text(mapResultOutput.toString().trim()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = new Job(conf, "CrystalBallStripe");
		job.setJarByClass(CrystalBallStripe.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputValueClass(MapWritable.class);

		job.setMapperClass(MapProcess.class);
		job.setReducerClass(ReduceProcess.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
