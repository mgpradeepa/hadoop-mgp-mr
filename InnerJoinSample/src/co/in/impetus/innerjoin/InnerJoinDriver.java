package co.in.impetus.innerjoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InnerJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		boolean success;

		Job job1 = new Job(getConf(), "Inner Join");
		job1.setJarByClass(getClass());
		/**
		 *  args[0] -> Driver File Name
		 *  args[1] -> Input path 
		 *  args[2] -> Output path  
		 */
		
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));

		job1.setMapperClass(InnerJoinMapper.class);
		job1.setReducerClass(InnerJoinReducer.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		success = job1.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new InnerJoinDriver(), args);
		System.exit(exitCode);
	}
}
