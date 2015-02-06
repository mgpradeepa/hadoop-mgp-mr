package in.co.impetus.usecases.poc.averageofnumbers;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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

	public static class CustomCombiner extends Reducer<Text, Text, Text, Text> {
		public CustomCombiner() {
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

			if(count > 1) {
				count--;
			}
			context.write(new Text("avg"), new Text(sum + "_" + count));

		};

		
	}

}
