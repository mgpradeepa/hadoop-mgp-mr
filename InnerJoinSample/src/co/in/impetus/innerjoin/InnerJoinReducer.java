package co.in.impetus.innerjoin;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *  Reducer class to perform the reducer functionality and joining the data from different input files which are combined as the part of mapper output
 *   
 * @author pradeep
 *
 */
public class InnerJoinReducer extends Reducer<LongWritable,Text,Text,Text> {

	private Set<String> aset = new HashSet<String>();
	private Set<String> cset = new HashSet<String>();
	private Text finalKey = new Text();
	private Text finalVal = new Text();

	
	
	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		// clearing all the intermediate datastructure for holding the values.
		aset.clear();
		cset.clear();
// loading to corresponding data holders a/c depending on the value being iterated
		for(Text value: values)
		{
			if(value!=null)
			{
				System.out.println("Test 1:---" + value.toString().substring(2));
				if(value.toString().startsWith("A:"))
				{
					aset.add(value.toString().substring(2));							
				}		
				if(value.toString().startsWith("C:"))
				{
					cset.add(value.toString().substring(2));
					//cset.add(null);
				}		
			}
		}
//		performs the actual join operation by iterating over the values.
		
		System.out.println("aset size is --->-" + aset.size() + " cset size is --->" + cset.size());
			for(String cstr:cset)
			{
				System.out.println(" cstr" +cstr);
				
				for(String astr:aset)
				{
					System.out.println(" astr" + astr);
				finalKey.clear();
				finalVal.clear();
				finalKey.set(cstr);
				finalVal.set(astr);
				context.write(finalKey, finalVal);
			}			
		}
	}
}