package com.impetus.movielens.uc.alumma;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class AllUserMaxMinAvgDriver extends MyJobConfiguration {
	public static void main(String[] args) {
		System.out
				.println("Finding max, min , avg rating where user has rated..!");

		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(configuration,
					new AllUserMaxMinAvgDriver(), args);
			System.out.println("Execution of Job completed");
		} catch (Exception e) {
			e.printStackTrace();

		}
		System.exit(exitCode);
	}

	public int run(String[] runArgs) throws Exception {

		getConf().set("io.sort.spill.percent", "0.80");
		Job job = getJobConfig();
		for (int i = 0; i < runArgs.length; i++) {
			System.out.println(runArgs[i]);

		}
		// /user/ubuntu/mgp/MovieLens/input/rating/ratings.dat
		// /user/ubuntu/mgp/MovieLens/output/op/alumma
		// runArgs[0] will be the class name that you would give in the cli
		String in = runArgs[0];
		String out = runArgs[1]; // /user/ubuntu/mgp/MovieLens/output/op/alumma

		FileInputFormat.addInputPaths(job, in);

		// FileSystem fs = FileSystem.getLocal(getConf());
		// fs.delete(new Path(out), true);

		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

		/**
		 * #Case1: If you set [job.setNumReduceTasks(0)] then it means no
		 * reducer is getting spawned and only the mapper output is the final
		 * output. Inference: The data is not sorted in the mappers. There are
		 * duplicates of the data in different mappers.
		 * 
		 * #Case2: If job.setNumReduceTasks(<greater than 0>) but no customized
		 * reducer written, then the identity reducer will be used. Inference:
		 * The data is sorted but has duplicate entries of the data. Output is
		 * one reducer file.
		 * 
		 * #Case3: If job.setNumReduceTasks(<greater than 0>) and the customized
		 * reducer is written, then the data output will be sorted on the basis
		 * of lexographical way
		 * 
		 */
		// job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	private Job getJobConfig() throws Exception {
		MyJobDefinition jobDefinition = new MyJobDefinition() {

			@Override
			public Class<? extends Reducer> getMyReducerClass() {
				// TODO Auto-generated method stub
				return AllUserMaxMinAvgReducer.class;
			}

			@Override
			public Class<? extends Partitioner> getMyPartitionerClass() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Class<?> getMyOutputValueClass() {
				// TODO Auto-generated method stub
				return Text.class;
			}

			@Override
			public Class<?> getMyOutputKeyClass() {
				// TODO Auto-generated method stub
				return Text.class;
			}

			@Override
			public Class<? extends Mapper> getMyMapperClass() {
				// TODO Auto-generated method stub
				return AllUserMaxMinAvgMapper.class;
			}

			@Override
			public Class<? extends Reducer> getMyCombinerClass() {
				return null;
			}

			@Override
			public Class<?> getJarByClass() {
				return this.getClass();
			}
		};

		Job job = setupJob(
				"Finding max, min , avg rating where user has rated ",
				jobDefinition);
		return job;
	}
}
