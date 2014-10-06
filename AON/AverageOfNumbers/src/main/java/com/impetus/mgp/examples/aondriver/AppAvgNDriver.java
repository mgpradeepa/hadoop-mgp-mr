package com.impetus.mgp.examples.aondriver;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.impetus.mgp.examples.aonmapper.AvgNMapper;
import com.impetus.mgp.examples.aonmapper.AvgNMapper.CustomCombiner;
import com.impetus.mgp.examples.aonreducer.AvgNReducer;
import com.impetus.mgp.examples.initconfig.MyJobConfiguration;

/**
 * 
 * @author mgp
 * <p>
 *         Driver to initiate the map reduce job with the required
 *         configurations setup done on the code level
 * 
 */
public class AppAvgNDriver extends MyJobConfiguration {

	public static void main(String[] args) {
		System.out.println("Finding Average started in optimized manner!");

		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(configuration, new AppAvgNDriver(), args);
			System.out.println("Execution of Job completed");
		} catch (Exception e) {
			e.printStackTrace();

		}
		System.exit(exitCode);
	}

	@Override
	public int run(String[] runArgs) throws Exception {

		getConf().set("io.sort.spill.percent", "0.80");
		Job job = getJobConfig();
		for (int i = 0; i < runArgs.length; i++) {
			System.out.println(runArgs[i]);

		}
		// runArgs[0] will be the class name that you would give in the cli

		// While running the program, the usage was through eclipse and
		// arguments were given in eclipse hence [0] and [1] has been used below

		String in = runArgs[0];
		String out = runArgs[1];

		FileInputFormat.addInputPath(job, new Path(in));

		FileSystem fs = FileSystem.getLocal(getConf());
		fs.delete(new Path(out), true);

		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	private Job getJobConfig() throws Exception {
		MyJobDefinition jobDefinition = new MyJobDefinition() {

			@Override
			public Class<? extends Reducer> getMyReducerClass() {
				return AvgNReducer.class;
			}

			@Override
			public Class<? extends Partitioner> getMyPartitionerClass() {
				return null;
			}

			@Override
			public Class<?> getMyOutputValueClass() {
				return Text.class;
			}

			@Override
			public Class<?> getMyOutputKeyClass() {
				return Text.class;
			}

			@Override
			public Class<? extends Mapper> getMyMapperClass() {
				return AvgNMapper.class;
			}

			@Override
			public Class<? extends Reducer> getMyCombinerClass() {
				return CustomCombiner.class;
			}

			@Override
			public Class<?> getJarByClass() {
				return this.getClass();
			}
		};

		Job job = setupJob("Average Number Job Optimized", jobDefinition);
		return job;
	}

}
