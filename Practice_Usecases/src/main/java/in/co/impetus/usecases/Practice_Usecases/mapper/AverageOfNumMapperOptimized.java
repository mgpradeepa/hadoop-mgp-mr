package in.co.impetus.usecases.Practice_Usecases.mapper;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageOfNumMapperOptimized extends
		Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		// System.out.println("value -> "+ value);
		context.write(new Text("Mapper"), value);

	};

	public class CustomCombiner extends
			Reducer<Text, Text, Text, Text> {
		protected void reduce(
				Text key,
				java.lang.Iterable<Text> values,
				Context context)
				throws java.io.IOException, InterruptedException {
			// TODO Need to change the keyout of the mapper and valuein to the
			// reducer
			
			Integer count =0;
			Double sum =0D;
			Iterator<Text> itr = values.iterator();
			while(itr.hasNext()) {
				Double val = Double.parseDouble(itr.next().toString());
				count++;
				sum+=val;
			}
			
			Double average = sum/count;
			context.write(new Text("A_C"), new Text(average+"_"+ count));

		};
	}
}
