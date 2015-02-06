package in.co.impetus.usecases.Practice_Usecases.driver;

import in.co.impetus.usecases.Practice_Usecases.entity.NewCustomer;
import in.co.impetus.usecases.Practice_Usecases.entity.OCRGroupComparator;
import in.co.impetus.usecases.Practice_Usecases.entity.OCRSortComparator;
import in.co.impetus.usecases.Practice_Usecases.entity.TwoCRSortComparator;
import in.co.impetus.usecases.Practice_Usecases.entity.TwoRGroupComparator;
import in.co.impetus.usecases.Practice_Usecases.mapper.TwoColumnRankingMapper;
import in.co.impetus.usecases.Practice_Usecases.partitioner.TwoColumnRankingPartitioner;
import in.co.impetus.usecases.Practice_Usecases.reducer.TwoColumnRankingReducer;

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

public class AppTwoColumnRankingDriver extends MyJobConfiguration {

	public static void main(String[] args) {
		System.out.println("Finding ranking of the scores and city of each customerid!");

		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(configuration,
					new AppTwoColumnRankingDriver(), args);
			System.out.println("Execution of Job completed");
		} catch (Exception e) {
			e.printStackTrace();

		}
		System.exit(exitCode);
	}

	public int run(String[] runArgs) throws Exception {

		// getConf().set("io.sort.spill.percent", "0.10");
		Job job = getJobConfig();
		for (int i = 0; i < runArgs.length; i++) {
			System.out.println(runArgs[i]);

		}
		String in = runArgs[0];
		String out = runArgs[1];

		FileInputFormat.addInputPath(job, new Path(in));

		FileSystem fs = FileSystem.getLocal(getConf());
		fs.delete(new Path(out), true);
		// recieved few times java heap space error so configured as below.
		getConf().set("mapred.map.child.java.opts", "-Xmx512m");
		getConf().set("mapred.reduce.child.java.opts", "-Xmx512m");

		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setSortComparatorClass(TwoCRSortComparator.class);
		job.setGroupingComparatorClass(TwoRGroupComparator.class);
		job.setMapOutputKeyClass(NewCustomer.class);
		job.setMapOutputValueClass(NewCustomer.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Customer.class);
		job.getConfiguration().setBoolean("mapreduce.map.speculative", false);
		job.getConfiguration().setInt("mapred.reduce.tasks",3);
		

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	private Job getJobConfig() throws Exception {
		MyJobDefinition jobDefinition = new MyJobDefinition() {

			@Override
			public Class<? extends Reducer> getMyReducerClass() {
				// TODO Auto-generated method stub
				return TwoColumnRankingReducer.class;
			}

			@Override
			public Class<? extends Partitioner> getMyPartitionerClass() {
				// TODO Auto-generated method stub
				return TwoColumnRankingPartitioner.class;
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
				return TwoColumnRankingMapper.class;
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
