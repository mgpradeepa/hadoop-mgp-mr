package com.impetus.movielens.uc.unrfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserNumberOfRatingForMovieReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(
			Text key,
			java.lang.Iterable<IntWritable> values,
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		int count = 0;
		for (IntWritable numberOfCounts : values) {
			count += numberOfCounts.get();

		}
		context.write(key, new IntWritable(count));

	};

}
