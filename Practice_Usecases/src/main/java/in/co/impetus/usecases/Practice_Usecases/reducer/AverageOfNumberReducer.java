package in.co.impetus.usecases.Practice_Usecases.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageOfNumberReducer extends
		Reducer<IntWritable, IntWritable, NullWritable, DoubleWritable> {

	DoubleWritable average = new DoubleWritable();
	NullWritable nu = NullWritable.get();
	protected void reduce(
			IntWritable key,
			java.lang.Iterable<IntWritable> values,
			Context context)
			throws java.io.IOException, InterruptedException {
		int sum =0;
		int count=0;
		for(IntWritable value : values) {
			sum+= value.get();
			count++;
			
		}
		average.set(sum/(double)count);

		context.write(nu, average);
	};

}
