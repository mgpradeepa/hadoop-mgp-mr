package com.impetus.mgp.examples.aonmapper;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author mgp
 *         <p>
 *         Mapper class having the implementation to asses the input file having
 *         numbers in it specified with \n separated
 * 
 */
public class AvgNMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = org.apache.commons.logging.LogFactory.getLog(AvgNMapper.class);

	/**
	 * have a static number you need to get the mod or remainder of the value
	 * across the track
	 * 
	 * 
	 */
	static int track = 1;

	Text outKey = new Text();
	NullWritable nul = NullWritable.get();

	/**
	 * <p>
	 * Creates custom keys with the values already present in the file by taking
	 * the mod of the value with some specific number from 1 to 10. So that for
	 * distribution with keys would be more effective.
	 * 
	 * <p>
	 * <p>
	 * The most customised way of doing this leveraging the HADOOP eco system is
	 * by using the counters and track them all at the end
	 * 
	 */
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		if (track > 11) {
			track = 1;
		} else
			track++;

		String val = value.toString();
		Integer convKey = Integer.parseInt(val) % track;
		log.info("Mod key -> " + convKey);
		outKey.set(new Text(String.valueOf(convKey)));

		// System.out.println("Key " + outKey + " - " + value);
		log.info("Key " + outKey + " - " + value);
		context.write(outKey, value);
		log.debug("Key " + outKey + " - " + value);

	};

	/**
	 * 
	 * @author mgp
	 *         <p>
	 * 
	 *         Instead of burdening all the reduction operation at the reducer
	 *         end; combine the values alloted to the particular set of keys by
	 *         calculating the count and sum value.
	 *         <p>
	 *         Easy for reducer to perform the reduction operation. By just
	 *         presenting the output value
	 * 
	 */
	public static class CustomCombiner extends Reducer<Text, Text, Text, Text> {
		public CustomCombiner() {
			// expects to have default constructor
		}

		protected void reduce(Text key, java.lang.Iterable<Text> values,
				Context context) throws java.io.IOException,
				InterruptedException {

			Integer count = 1;
			Double sum = 0D;
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				Double val = Double.parseDouble(itr.next().toString());
				count++;
				sum += val;
			}

			if (count > 1) {
				count--;
			}
			// Combine all the reduction to one key and provide to the reducer
			// to just do the deduction operation
			context.write(new Text("avg"), new Text(sum + "_" + count));

		};

	}

}
