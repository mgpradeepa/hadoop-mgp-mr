package com.impetus.movielens.uc.urmo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingMovieOnceReducer extends
		Reducer<Text, NullWritable, Text, NullWritable> {

	protected void reduce(
			Text key,
			java.lang.Iterable<NullWritable> values,
			org.apache.hadoop.mapreduce.Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		
		
		context.write(key, NullWritable.get());

	};

}
