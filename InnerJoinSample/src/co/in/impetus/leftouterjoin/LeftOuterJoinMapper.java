package co.in.impetus.leftouterjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class LeftOuterJoinMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	Text val = new Text();

	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		String record[] = value.toString().split(",");

		FileSplit fs = (FileSplit) context.getInputSplit();
		String path = fs.getPath().toString();

		if (path.contains("attributes")) {
			val.set("A:" + record[1]);
			Long ll = (Long.parseLong(record[0]));
			LongWritable l1key = new LongWritable(ll.longValue());
			context.write(l1key, val);
		} else if (path.contains("customer")) {
			val.set("C:" + record[0]);
			Long ll = (Long.parseLong(record[1]));
			LongWritable l1key = new LongWritable(ll.longValue());
			context.write(l1key, val);
		}

	};

}
