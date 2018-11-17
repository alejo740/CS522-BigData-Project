package cs522.part5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapProcess extends Mapper<LongWritable, Text, NullWritable, Pair> {

	Pair distanceAndModel = new Pair();
	TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
	int K;

	double normalisedSAge;
	double normalisedSIncome;
	String sStatus;
	String sGender;
	double normalisedSChildren;
	
	double minAge = 18;
	double maxAge = 77;
	double minIncome = 5000;
	double maxIncome = 67789;
	double minChildren = 0;
	double maxChildren = 5;

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Pair>.Context context)
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
			normalisedSAge = Util.normalisedDouble(st.nextToken(), minAge, maxAge);
			normalisedSIncome = Util.normalisedDouble(st.nextToken(), minIncome, maxIncome);
			sStatus = st.nextToken();
			sGender = st.nextToken();
			normalisedSChildren = Util.normalisedDouble(st.nextToken(), minChildren,
					maxChildren);
			
			input.close();
			myReader.close();
		}

	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String rLine = value.toString();
		StringTokenizer st = new StringTokenizer(rLine, ",");

		double normalisedRAge = Util.normalisedDouble(st.nextToken(), minAge, maxAge);
		double normalisedRIncome = Util.normalisedDouble(st.nextToken(), minIncome,
				maxIncome);
		String rStatus = st.nextToken();
		String rGender = st.nextToken();
		double normalisedRChildren = Util.normalisedDouble(st.nextToken(),
				minChildren, maxChildren);
		String rModel = st.nextToken();

		double tDist = Util.totalSquaredDistance(normalisedRAge, normalisedRIncome,
				rStatus, rGender, normalisedRChildren, normalisedSAge,
				normalisedSIncome, sStatus, sGender, normalisedSChildren);

		KnnMap.put(tDist, rModel);

		if (KnnMap.size() > K) {
			KnnMap.remove(KnnMap.lastKey());
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, NullWritable, Pair>.Context context)
			throws IOException, InterruptedException {

		for (Map.Entry<Double, String> entry : KnnMap.entrySet()) {
			Double knnDist = entry.getKey();
			String knnModel = entry.getValue();

			distanceAndModel.set(knnDist, knnModel);

			context.write(NullWritable.get(), distanceAndModel);
		}

	}

}
