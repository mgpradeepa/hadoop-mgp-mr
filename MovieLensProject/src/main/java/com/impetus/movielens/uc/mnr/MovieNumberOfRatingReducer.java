package com.impetus.movielens.uc.mnr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieNumberOfRatingReducer extends
		Reducer<LongWritable, Text, Text, LongWritable> {

	protected void reduce(
			LongWritable key,
			java.lang.Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, Text, LongWritable>.Context context)
			throws java.io.IOException, InterruptedException {

		Text movieName = new Text();
		int count = 0;
		movieName.clear();
		for (Text movie : values) {
			String tempMovie = movie.toString();
			System.out.println(tempMovie);
			if (tempMovie.matches("\\d+(\\.?\\d*)")) {
//				System.out.println("Integer -" + tempMovie );

				count++;
			} else {
				movieName.set(movie + "->>");
			}
		}
		if (movieName.getLength() > 0) {
//			System.out.println("#" + movieName + ":" + count);
			context.write(movieName, new LongWritable(count));
		}

	};

}
/*
 * 
 * int count = 0;
		// what if multi movies names are mapped to same movie id?
		Text movieName = new Text();
		movieName.clear();
		for (Text objectWritable : values) {
			// what we have in values is the list of movie names & ratings
			String value = objectWritable.toString();
			if (value.matches("\\d+(\\.?\\d*)")) {
				// it is an integer
				count++;
			} else {
				movieName.set(objectWritable);
			}
		}
		if (movieName.getLength() > 0) {
			System.out.println("#"+movieName+":"+count);
			context.write(movieName, new LongWritable(count));
		}
 */
