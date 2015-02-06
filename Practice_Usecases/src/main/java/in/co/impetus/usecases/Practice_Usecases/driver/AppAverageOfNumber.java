package in.co.impetus.usecases.Practice_Usecases.driver;

import in.co.impetus.usecases.Practice_Usecases.mapper.AverageOfNumberMapper;
import in.co.impetus.usecases.Practice_Usecases.reducer.AverageOfNumberReducer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hello world!
 * 
 */
public class AppAverageOfNumber extends MyJobConfiguration {

	public static void main(String[] args) {
		System.out.println("Finding Average started!");

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

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	private Job getJobConfig() throws Exception {
		MyJobDefinition jobDefinition = new MyJobDefinition() {

			@Override
			public Class<? extends Reducer> getMyReducerClass() {
				// TODO Auto-generated method stub
				return AverageOfNumberReducer.class;
			}

			@Override
			public Class<? extends Partitioner> getMyPartitionerClass() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public Class<?> getMyOutputValueClass() {
				// TODO Auto-generated method stub
				return DoubleWritable.class;
			}

			@Override
			public Class<?> getMyOutputKeyClass() {
				// TODO Auto-generated method stub
				return IntWritable.class;
			}

			@Override
			public Class<? extends Mapper> getMyMapperClass() {
				// TODO Auto-generated method stub
				return AverageOfNumberMapper.class;
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

		Job job = setupJob("Average Number Job", jobDefinition);
		return job;
	}
}
