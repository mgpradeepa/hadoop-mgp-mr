package in.co.impetus.sample.rsj.partitioner;

import in.co.impetus.sample.rsj.entity.CompositeKeyWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RSJPartitioner extends Partitioner<CompositeKeyWritable, Text> {
	@Override
	public int getPartition(CompositeKeyWritable key, Text value,
			int numPartitions) {
		// do partition on join key that is employee id
		return (key.getJoinKey().hashCode() % numPartitions);// numPartitions = numReduceTasks 
	}

}
