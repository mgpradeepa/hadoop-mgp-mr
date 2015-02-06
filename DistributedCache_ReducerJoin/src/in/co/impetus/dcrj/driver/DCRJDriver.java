package in.co.impetus.dcrj.driver;

import in.co.impetus.dcrj.mapper.DCRJMapper;
import in.co.impetus.dcrj.reducer.DCRJReducer;

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

public class DCRJDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());

		System.out.println("Run check1");
//		StringBuilder inputPaths = new StringBuilder();
//		inputPaths.append(args[0].toString());
		System.out.println("Run check2");
		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileInputFormat.setInputPaths(job, inputPaths.toString());
		
//		Path outDirectory = new Path(args[1]);
//		FileSystem fs = FileSystem.get(conf);
//		fs.delete(outDirectory, true);
		System.out.println("Run check3");
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(in.co.impetus.dcrj.driver.DCRJDriver.class);
		job.setJobName("Distributed cache design ");

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// conf.setStrings("attributes", "A");
		// conf.setStrings("customer", "C");

		job.setMapperClass(DCRJMapper.class);
		job.setReducerClass(DCRJReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.out.println("Run check4");

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int extit = ToolRunner.run(new Configuration(), new DCRJDriver(), args);
		System.exit(extit);

	}

}
