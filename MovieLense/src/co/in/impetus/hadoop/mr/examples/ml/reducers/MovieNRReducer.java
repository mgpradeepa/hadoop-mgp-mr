package co.in.impetus.hadoop.mr.examples.ml.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieNRReducer extends
		Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	protected void reduce(
			LongWritable inputFromMap,
			java.lang.Iterable<LongWritable> iterator,
			Context context)
			throws java.io.IOException, InterruptedException {

//		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
//		System.out.println("method caller -> "+ " :: " +stackTraceElements[0] +" :: " +stackTraceElements[1] + " :: " +stackTraceElements[2] + " :: " +stackTraceElements[3] + " :: " +stackTraceElements[4] );
		
//		System.out.println(" Key --> "+ context.getCurrentKey() + " ::  -> Value -> " + context.getValues());
		long inter = 0;
		for(LongWritable count: iterator){
//			inter+= count.get() ;
			inter += count.get();
		}
		context.write(inputFromMap, new LongWritable(inter));		
//		System.out.println(inputFromMap + ":: "+inter);
		
		
		
	}
}
