package com.impetus.mgp.examples.initconfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

/**
 * 
 * @author mgp
 * <p>
 *         Job configuration details are provided in an abstract way by extending the basic classes {@link Configured} 
 *         and implementing {@link Tool}
 */
public abstract class MyJobConfiguration extends Configured implements Tool {
	protected static Configuration configuration = null;
	static {
		configuration = new Configuration();
	}

	protected Job setupJob(String jobName, MyJobDefinition jobDefinition)
			throws Exception {

		Job job = new Job(configuration, jobName);

		job.setJarByClass(jobDefinition.getJarByClass());
		job.setMapperClass(jobDefinition.getMyMapperClass());
		// combiner is not mandatory and can be null
		if (jobDefinition.getMyCombinerClass() != null) {
			job.setCombinerClass(jobDefinition.getMyCombinerClass());
		}
		if (jobDefinition.getMyPartitionerClass() != null) {
			job.setPartitionerClass(jobDefinition.getMyPartitionerClass());
		}
		job.setReducerClass(jobDefinition.getMyReducerClass());
		job.setOutputKeyClass(jobDefinition.getMyOutputKeyClass());
		job.setOutputValueClass(jobDefinition.getMyOutputValueClass());

		return job;
	}

	public int run(String[] arg0) throws Exception {
		// The implementation is specific to the use case. Hence the specific
		// implementation is done by overriding this method in particular driver
		// class
		return 0;
	}

	/**
	 * 
	 * @author mgp 
	 * 
	 * <p>
	 * These are the definite requirement for any map reduce job.
	 *         The details are provided in particular to the driver and its
	 *         requisites
	 * <p>
	 *         This class has to be  with access specifier protected as only the Driver should know the
	 *         details of the requisite configurations.
	 * 
	 */
	protected abstract class MyJobDefinition {
		public abstract Class<?> getJarByClass();

		public abstract Class<? extends Mapper> getMyMapperClass();

		public abstract Class<? extends Reducer> getMyReducerClass();

		public abstract Class<? extends Reducer> getMyCombinerClass();

		public abstract Class<? extends Partitioner> getMyPartitionerClass();

		public abstract Class<?> getMyOutputKeyClass();

		public abstract Class<?> getMyOutputValueClass();
	}

}
