package in.co.impetus.usecases.poc.rankingcustomerone;



import in.co.impetus.usecases.poc.init.MyJobConfiguration;

import org.apache.hadoop.fs.FileSystem;
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

public class AppOneColumnRankingDriver extends MyJobConfiguration {

	public static void main(String[] args) {
		System.out.println("Finding ranking of the scores of each customerid!");

		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(configuration,
					new AppOneColumnRankingDriver(), args);
			System.out.println("Execution of Job completed");
		} catch (Exception e) {
			e.printStackTrace();

		}
		System.exit(exitCode);
	}

	public int run(String[] runArgs) throws Exception {

		Job job = getJobConfig();
		for (int i = 0; i < runArgs.length; i++) {
			System.out.println(runArgs[i]);

		}
		String in = runArgs[0];
		String out = runArgs[1];

		FileInputFormat.addInputPath(job, new Path(in));

		FileSystem fs = FileSystem.getLocal(getConf());
		fs.delete(new Path(out), true);

		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setSortComparatorClass(OCRSortComparator.class);
		job.setGroupingComparatorClass(OCRGroupComparator.class);
		job.setMapOutputKeyClass(Customer.class);
		job.setMapOutputValueClass(Customer.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Customer.class);
		job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	private Job getJobConfig() throws Exception {
		MyJobDefinition jobDefinition = new MyJobDefinition() {

			@Override
			public Class<? extends Reducer> getMyReducerClass() {
				// TODO Auto-generated method stub
				return OneColumnRankingReducer.class;
			}

			@Override
			public Class<? extends Partitioner> getMyPartitionerClass() {
				// TODO Auto-generated method stub
				return OneColumnRankingPartitioner.class;
			}

			@Override
			public Class<?> getMyOutputValueClass() {
				// TODO Auto-generated method stub
				return Text.class;
			}

			@Override
			public Class<?> getMyOutputKeyClass() {
				// TODO Auto-generated method stub
				return NullWritable.class;
			}

			@Override
			public Class<? extends Mapper> getMyMapperClass() {
				// TODO Auto-generated method stub
				return OneColumnRankingMapper.class;
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

		Job job = setupJob("Ranking identified", jobDefinition);
		return job;
	}

}
