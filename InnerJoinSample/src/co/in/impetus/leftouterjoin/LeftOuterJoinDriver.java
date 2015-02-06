/**
 * 
 */
package co.in.impetus.leftouterjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import co.in.impetus.innerjoin.InnerJoinMapper;
import co.in.impetus.innerjoin.InnerJoinReducer;

/**
 * @author ubuntu
 *
 */
public class LeftOuterJoinDriver extends Configured implements Tool {

	boolean success = false;
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] arg) throws Exception {

		Job job1 = new Job(getConf(), "Left Outer Join");
		job1.setJarByClass(getClass());
		FileInputFormat.addInputPath(job1, new Path(arg[1]));		
		FileOutputFormat.setOutputPath(job1, new Path(arg[2]));

	    job1.setMapperClass(LeftOuterJoinMapper.class);
		job1.setReducerClass(LeftOuterJoinReducer.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
	

		success = job1.waitForCompletion(true);


		return success ? 0 : 1;
		
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(), new LeftOuterJoinDriver(), args);
		System.exit(exitCode);
		
	}

}
