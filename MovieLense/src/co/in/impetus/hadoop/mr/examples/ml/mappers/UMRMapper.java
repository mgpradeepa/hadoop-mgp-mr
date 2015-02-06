package co.in.impetus.hadoop.mr.examples.ml.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UMRMapper {
	// Input format : <LongWritable: offset, Text: Movie Name> << Movie ID :
	// Text, Movie Name>>
	//*//TODO : Below codes needs to be adjusted according to 2nd requirement
	
	
	
	public static class MovieMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			String[] movieLine = value.toString().split("::");
			Text movieData = new Text();
			movieData.set(movieLine[1]);

			context.write(new Text(movieLine[0]), movieData);

		}

	}

	/**
	 * 
	 * @author ubuntu
	 * 
	 * 
	 */
	// data format from rating.dat: [UserId:: MovieId:: Rating:: Time]
	public static class RatingToMovieMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			String[] eachLine = value.toString().split("::");
			Text movieRatingData = new Text();
			movieRatingData.set(eachLine[0] + "::" + eachLine[2]);

			context.write(new Text(eachLine[1]), movieRatingData);

		}

	}

}
