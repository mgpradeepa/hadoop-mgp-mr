package in.co.impetus.usecases.Practice_Usecases.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageOfNumberMapper extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	IntWritable intWritable = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		// System.out.println("value -> "+ value);
		context.write(intWritable,
				new IntWritable(Integer.parseInt(value.toString())));
		//1, 2
		//1, 3
		//1,4
	

	};

}

class AverageOfNumberCombiner extends
		Reducer<IntWritable, IntWritable, NullWritable, DoubleWritable> {
	protected void reduce(
			IntWritable key,
			java.lang.Iterable<IntWritable> values,
			org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, NullWritable, DoubleWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		// TODO Need to change the keyout of the mapper and valuein to the
		// reducer

	};
}
