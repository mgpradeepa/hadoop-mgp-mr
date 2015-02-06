package in.co.impetus.sample.rsj.entity;

import in.co.impetus.sample.rsj.driver.DriverRSJ;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducersideJoinRSJ extends
		Reducer<CompositeKeyWritable, Text, NullWritable, Text> {

	StringBuilder reduceValueBuilder = new StringBuilder("");
	NullWritable nullWritable = NullWritable.get();

	Text reduceOutputValue = new Text("");
	String separator = ",";
	private MapFile.Reader deptMapReader = null;

	Text txtMapFileLookpKey = new Text("");
	Text txtMapFileLookpValue = new Text("");

	protected void setup(
			org.apache.hadoop.mapreduce.Reducer<CompositeKeyWritable, Text, NullWritable, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		// Get side data from the distributed cache
		Path[] cacheFileLocal = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (URI eachPath : DriverRSJ.cacheLocalFiles) {
			// for (URI eachPath : DriverRSJ.uriAsDC) {
			System.out.println(eachPath);
			if (eachPath.getPath().endsWith("departments_map.txt")) {
				URI uriFile;
				try {
					uriFile = new URI(
							"hdfs",
							null,
							"localhost",
							54310,
							"/user/ubuntu/mgp/JoinProject/input/Empl_Dept/withsalData/REdJoin_Df/departments_map.txt",
							null, null);
					initializeDepartmentsMap(uriFile, context);
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// URI uriFile =
				// URI.create("hdfs://localhost:54310/user/ubuntu/mgp/JoinProject/input/Empl_Dept/withsalData/REdJoin_Df/departments_map");

			}
		}
	}

	private void initializeDepartmentsMap(URI uriFile,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException {

		// Initialize the reader
		FileSystem dfs = FileSystem.get(context.getConfiguration());
		try {
			System.out.println("DFS -> "+dfs + " ::  URIFILE -> " + uriFile);
			deptMapReader = new MapFile.Reader(dfs, uriFile.toString().substring(0, uriFile.toString().length()-5),
					context.getConfiguration());
		} catch (Exception e) {
			e.printStackTrace();

		}

	};

	protected void reduce(CompositeKeyWritable key,
			java.lang.Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Iterate through values; First set is csv of employee data
		// second set is salary data; The data is already ordered
		// by virtue of secondary sort; Append each value;

		for (Text value : values) {
			buildOutputValue(key, reduceValueBuilder, value);
		}
		// Drop last comma , set value, and emit the output

		if (reduceValueBuilder.length() > 1) {
			reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
			context.write(nullWritable, reduceOutputValue);
		} else {
			System.out.println("Key=" + key.getJoinKey() + "src="
					+ key.getSourceIndex());
		}
		// Reset variables
		reduceValueBuilder.setLength(0);
		reduceOutputValue.set("");

	}

	protected void cleanup(
			org.apache.hadoop.mapreduce.Reducer<CompositeKeyWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		deptMapReader.close();

	};

	private StringBuilder buildOutputValue(CompositeKeyWritable key,
			StringBuilder reduceValueBuilder, Text value) {
		if (key.getSourceIndex() == 1) {
			// Employee data
			// {{
			// Get the department name from the MapFile in distributedCache

			// Insert the joinKey (empNo) to beginning of the stringBuilder
			reduceValueBuilder.append(key.getJoinKey()).append(separator);

			String arrEmpAttributes[] = value.toString().split(",");
			txtMapFileLookpKey.set(arrEmpAttributes[3].toString());

			try {
				deptMapReader.get(txtMapFileLookpKey, txtMapFileLookpValue);
			} catch (Exception e) {
				txtMapFileLookpValue.set("");
			} finally {
				txtMapFileLookpValue
						.set((txtMapFileLookpValue.equals(null) || txtMapFileLookpValue
								.equals("")) ? "NOT FOUND"
								: txtMapFileLookpValue.toString());
			}
			// }}

			// {{
			// Append the department name to the map values to form a complete
			// CSV of employee attributes

			reduceValueBuilder.append(value.toString()).append(separator)
					.append(txtMapFileLookpValue.toString()).append(separator);
		} else if (key.getSourceIndex() == 2) {
			// Current recent salary data (1..1 on join key)
			// Salary data; Just append the salary, drop the effective-to-date
			String[] arrSalAttribValue = value.toString().split(",");
			reduceValueBuilder.append(arrSalAttribValue[0].toString()).append(
					separator);
		} else if (key.getSourceIndex() != 4) { // key for historical data

			String arrSalAttributes[] = value.toString().split(",");
			if (arrSalAttributes[1].toString().equals("9999-01-01")) {
				reduceValueBuilder.append(arrSalAttributes[0].toString())
						.append(separator);
			}
		}
		// }}

		// Reset
		txtMapFileLookpKey.set("");
		txtMapFileLookpValue.set("");

		return reduceValueBuilder;
	};

}
