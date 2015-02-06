package in.co.impetus.usecases.poc.averageofnumbers;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgNReducer extends Reducer<Text, Text, Text, Text> {
	Log log = LogFactory.getLog(AvgNReducer.class);
	Text outValue = new Text("");
	NullWritable nw = NullWritable.get();

	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {

		Iterator<Text> itr = values.iterator();

		double sum = 0;
		Integer count = 0;
		Double average = 0D;
		double checkCount = 0D;

		String[] inter;
		while (itr.hasNext()) {
			checkCount++;
			inter = itr.next().toString().split("_");
			sum += Double.parseDouble(inter[0]);
			count += Integer.parseInt(inter[1]);
			log.info(sum + " " + count);
			;

			log.info("Average in for loop " + average);

		}
		average = sum / (double) count;

		context.write(outValue, new Text(average.toString()));

	};

}
