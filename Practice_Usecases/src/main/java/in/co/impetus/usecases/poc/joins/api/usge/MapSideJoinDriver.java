package in.co.impetus.usecases.poc.joins.api.usge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoinDriver extends Configured implements Tool {
//	public static Configuration conf = null;
//	static {
//		conf = new Configuration();
//
//	}

	/*
	 * rw-r--r-- 1 ubuntu supergroup 56 2014-11-10 15:53
	 * /user/ubuntu/mgp/Jp/mapsidejoin/inputs/fileA.txt -rw-r--r-- 1 ubuntu
	 * supergroup 46 2014-11-10 15:53
	 * /user/ubuntu/mgp/Jp/mapsidejoin/inputs/fileB.txt
	 * ubuntu@ubuntu:~/Public/MGP/DataFolder/Joinapi$ hadoop fs -mkdir
	 * /user/ubuntu/mgp/Jp/mapsidejoin/outputs
	 */

	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run( new MapSideJoinDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {

		org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(
				getConf(), "map Side join Job");
		job.setJarByClass(MapSideJoinDriver.class);

		Path p1 = new Path(args[0]);
		Path p2 = new Path(args[1]);
		Path out = new Path(args[2]);

		job.setInputFormatClass(CompositeInputFormat.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.getConfiguration().set(
				CompositeInputFormat.JOIN_EXPR,
				CompositeInputFormat.compose("inner",
						KeyValueTextInputFormat.class, p1, p2));

		// set reducers to 0
		job.setNumReduceTasks(0);
		TextOutputFormat.setOutputPath(job, out);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
