package in.co.impetus.usecases.Practice_Usecases.driver;

import in.co.impetus.usecases.Practice_Usecases.mapper.AverageOfNumMapperOptimized;
import in.co.impetus.usecases.Practice_Usecases.mapper.AverageOfNumMapperOptimized.CustomCombiner;
import in.co.impetus.usecases.Practice_Usecases.reducer.AverageOfNumReducerOptimized;

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

public class AppAverageNumOptimized extends MyJobConfiguration {


	public static void main(String[] args) {
		System.out.println("Finding Average started in optimized manner!");

		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(configuration, new AppAverageOfNumber(),
					args);
			System.out.println("Execution of Job completed");
		} catch (Exception e) {
			e.printStackTrace();

		}
		System.exit(exitCode);
	}

	public int run(String[] runArgs) throws Exception {

		getConf().set("io.sort.spill.percent", "0.10");
		Job job = getJobConfig();
		for(int i =0; i < runArgs.length; i++) {
			System.out.println(runArgs[i]);
			
		}
		// runArgs[0] will be the class name that you would give in the cli
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
				// TODO Auto-generated method stub
				return AverageOfNumReducerOptimized.class;
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
				return AverageOfNumMapperOptimized.class;
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
