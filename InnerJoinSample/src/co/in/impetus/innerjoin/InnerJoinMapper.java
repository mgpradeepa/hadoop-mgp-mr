package co.in.impetus.innerjoin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Mapper class where the map functionality is executed. For Joining two
 * datasets the mapper is providing the sanctity by modulating the data so that
 * the reducer can join the requisite based on the custom condition
 * 
 * 
 * In this class {@link InnerJoinMapper} the Joining functionality is not done.
 * Joining is the responsibility of the reducer.
 * 
 * @author pradeep
 * 
 */
public class InnerJoinMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	// String prefix = null;
	Text tval = new Text();

	// Set<Text> txt1 = new HashSet<Text>();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] strArr = value.toString().split(",");

		// uses file split to get the details of the file.
		// Depending on the name of the file in the path of the split the
		// further decision of the mapper is taken

		FileSplit is = (FileSplit) context.getInputSplit();
		String path = is.getPath().toString();
		if (path.contains("attributes")) { // here 'attributes' is the name of
											// the file
			// String vv = strArr[0].concat(":").concat(strArr[1]);
			/**
			 * differentiate the value of the file data by prepending the file name of its origin
			 * hence 'A:' here
			 */
			tval.set("A:" + strArr[1]);
			Long ll = (Long.parseLong(strArr[0]));
			LongWritable l1key = new LongWritable(ll.longValue());
			context.write(l1key, tval);
		} else if (path.contains("customer")) {// here 'customer' is the name of
												// the file
			/**
			 * differentiate the value of the file data by prepending the file name of its origin
			 * hence 'C:' here
			 */
			tval.set("C:" + strArr[0]);
			
			Long ll = (Long.parseLong(strArr[1]));
			LongWritable l1key = new LongWritable(ll.longValue());
			context.write(l1key, tval);
		}
	}

}
