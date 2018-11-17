package cs522.part5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceProcess extends
		Reducer<NullWritable, Pair, NullWritable, Text> {

	private TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
	private int K;

	@Override
	protected void setup(
			Reducer<NullWritable, Pair, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String data = conf.get("inputParams");
		if (data == null) {
			return;
		}

		FileSystem dfs = FileSystem.get(conf);
		Path path = new Path(data);
		FileSystem fs = FileSystem.get(conf);
		FileStatus fstatus[] = fs.listStatus(path);

		for (FileStatus f : fstatus) {

			FSDataInputStream input = dfs.open(f.getPath());
			BufferedReader myReader = new BufferedReader(new InputStreamReader(input));

			String knnParams = myReader.readLine();

			StringTokenizer st = new StringTokenizer(knnParams, ",");

			K = Integer.parseInt(st.nextToken());

			input.close();
			myReader.close();
		}
		
		
		
	}

	@Override
	protected void reduce(NullWritable key, Iterable<Pair> values,
			Reducer<NullWritable, Pair, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		for (Pair val : values) {
			String rModel = val.getValue();
			double tDist = val.getKey();
			
			KnnMap.put(tDist, rModel);
			if (KnnMap.size() > K) {
				KnnMap.remove(KnnMap.lastKey());
			}
		}

		List<String> knnList = new ArrayList<String>(KnnMap.values());

		Map<String, Integer> freqMap = new HashMap<String, Integer>();

		for (int i = 0; i < knnList.size(); i++) {
			Integer frequency = freqMap.get(knnList.get(i));
			if (frequency == null) {
				freqMap.put(knnList.get(i), 1);
			} else {
				freqMap.put(knnList.get(i), frequency + 1);
			}
		}

		String mostCommonModel = null;
		int maxFrequency = -1;
		for (Map.Entry<String, Integer> entry : freqMap.entrySet()) {
			if (entry.getValue() > maxFrequency) {
				mostCommonModel = entry.getKey();
				maxFrequency = entry.getValue();
			}
		}
		// Use this line to produce a single classification
		context.write(NullWritable.get(), new Text(mostCommonModel));
	}

}
