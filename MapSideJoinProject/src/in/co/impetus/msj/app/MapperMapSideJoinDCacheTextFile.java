package in.co.impetus.msj.app;

import in.co.impetus.msj.driver.DriverMapSideJoinDCacheTxtFile;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperMapSideJoinDCacheTextFile extends
		Mapper<LongWritable, Text, Text, Text> {

	public static HashMap<String, String> departmentHashMap = new HashMap<String, String>();

	private BufferedReader bufferedReader;
	private String strDepName = "";
	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");

	// Custom static Counters for getting the information of the error scenarios
	// of the data processing in the mapper
	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}

	/**
	 * setup() method is run before the mapper method is actually called. Here
	 * the {@link DistributedCache } is used. The small chunk file of department
	 * which is the lookup file is distributed during the setup to all the nodes
	 * which mapper work would seek through.
	 * 
	 */
	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {

		for (URI eachPath : DriverMapSideJoinDCacheTxtFile.cacheLocalFiles) {
			boolean equals = eachPath.getPath().endsWith("dept.txt");
			if (equals) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadDepartmentCache(new Path(eachPath.getPath()), context);
			}
		}

	}

	/**
	 * 
	 * @param filePath
	 *            path of the input split file. Here it is specifically for
	 *            cache file reading and loading to the temporary data structure
	 *            for easy implmenentation
	 * 
	 * @param context
	 *            {@link Mapper.Context}
	 * @throws IOException
	 */
	private void loadDepartmentCache(Path filePath,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException {

		String strLineRead = "";
		try {
			bufferedReader = new BufferedReader(new FileReader(
					filePath.toString()));

			// Read each line and split and load to hashmap
			while ((strLineRead = bufferedReader.readLine()) != null) {
				String deptFieldArray[] = strLineRead.split("\\t");
				departmentHashMap.put(deptFieldArray[0].trim(),
						deptFieldArray[1].trim());
			}
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);

		} catch (IOException ioe) {
			ioe.printStackTrace();
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
		} finally {
			if (bufferedReader != null) {
				bufferedReader.close();
			}
		}

	};

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
		String val = value.toString();
		if (val.length() > 0) {
			String arrEmpAttr[] = val.split("\\t");

			try {
				strDepName = departmentHashMap.get(arrEmpAttr[4].toString());
			} finally {
				strDepName = (strDepName.equals(null) || strDepName.equals("")) ? "NOT FOUND"
						: strDepName;
			}

			txtMapOutputKey.set(arrEmpAttr[0].toString());
			txtMapOutputValue.set(arrEmpAttr[1].toString() + "\t"
					+ arrEmpAttr[2].toString() + "\t"
					+ arrEmpAttr[3].toString() + "\t"
					+ arrEmpAttr[4].toString() + "\t" + strDepName);
		}
		context.write(txtMapOutputKey, txtMapOutputValue);
		strDepName = "";
	};

}
