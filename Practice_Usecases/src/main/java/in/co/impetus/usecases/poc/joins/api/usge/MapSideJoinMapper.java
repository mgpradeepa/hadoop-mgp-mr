package in.co.impetus.usecases.poc.joins.api.usge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class MapSideJoinMapper extends Mapper<Text, TupleWritable, Text, Text> {

	protected void map(
			Text key,
			TupleWritable value,
			org.apache.hadoop.mapreduce.Mapper<Text, TupleWritable, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		context.write((Text) value.get(0), (Text) value.get(1));
	};
}
