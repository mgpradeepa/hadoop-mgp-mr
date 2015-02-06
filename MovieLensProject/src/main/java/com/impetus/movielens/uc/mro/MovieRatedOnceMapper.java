package com.impetus.movielens.uc.mro;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatedOnceMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	final String splitter = "::" ;
	final String separator = "_" ;
	Text movieIdKey = new Text();
	String uidMid = null;
	NullWritable nulValue = NullWritable.get();
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		
		String[] lineRecord = value.toString().split(splitter);
		movieIdKey.set(lineRecord[1]);
		
		
//		context.write(key, value)
//		context.write(userId_movieId_key, ONE);
		context.write(movieIdKey, nulValue);
		
		
	};

}
