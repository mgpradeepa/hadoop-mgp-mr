package in.co.impetus.usecases.Practice_Usecases.reducer;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageOfNumReducerOptimized extends
		Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {

		Double sum = 0D;
		Integer totalCount = 0;
		final Iterator<Text> itr = values.iterator();
		while (itr.hasNext()) {
			final String text = itr.next().toString();
			final String[] tokens = text.split("_");
			final Double average = Double.parseDouble(tokens[0]);
			final Integer count = Integer.parseInt(tokens[1]);
			sum += (average * count);
			totalCount += count;
		}

		final Double average = sum / totalCount;

		context.write(new Text("AVERAGE"), new Text(average.toString()));

	};

}
