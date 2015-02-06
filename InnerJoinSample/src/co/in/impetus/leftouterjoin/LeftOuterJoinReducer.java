package co.in.impetus.leftouterjoin;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LeftOuterJoinReducer extends
		Reducer<LongWritable, Text, Text, Text> {
	private Set<String> attribSet = new HashSet<String>();
	private Set<String> custSet = new HashSet<String>();

	private Text finalKey = new Text();
	private Text finalValue = new Text();

	Text EMPTY_Text = null;
	protected void reduce(
			LongWritable key,
			java.lang.Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		
		attribSet.clear();
		custSet.clear();
		
			for (Text value : values) {
			if (value != null) {
				System.out
						.println("Test 1:---" + value.toString().substring(2));
				if (value.toString().startsWith("A:")) {
					attribSet.add(value.toString().substring(2));
				}
				if (value.toString().startsWith("C:")) {
					custSet.add(value.toString().substring(2));
				}
			}
		}
			
			
			/**
			 * ANY ONE OF THE BELOW JOINS SHOULD BE RUN AT ONCE
			 */
			// Actual outer join happens here by exclusion of the matching by replacing them with the "NULL" value
/**
 *  LEFT OUTER JOIN
 */
			/**
			 * START LEFT O J
			 */
//			for(String attributes: attribSet) {
//				if(!custSet.isEmpty()) {
//					for(String custo : custSet ){
//						finalKey.clear();
//						finalValue.clear();
//						finalKey.set(attributes);
//						finalValue.set(custo);
//						context.write(finalKey, finalValue);
//						
//					}
//				}
//				else{
//					finalKey.clear();
//					finalValue.clear();					
//					finalKey.set(attributes);
//					context.write(finalKey, EMPTY_Text);
//					
//					
//				}
//			}
			
			/**
			 * START RIGHT O J
			 */
			/**
			 *  Right Outer Join
			 */
			
//			for(String custo: custSet) {
//				if(!attribSet.isEmpty()) {
//					for(String attributes : attribSet ){
//						finalKey.clear();
//						finalValue.clear();
//						finalKey.set( custo);
//						finalValue.set(attributes);
//						context.write(finalKey, finalValue);
//						
//					}
//				}
//				else{
//					finalKey.clear();
//					finalValue.clear();					
//					finalKey.set(custo);
//					context.write(finalKey, EMPTY_Text);
//					
//					
//				}
//			}
			
			/**
			 * START FULL O J
			 */
			// FULL OUTER JOIN
			for(String custo: custSet) {
				if(!custSet.isEmpty()) {
					
					for(String attributes : attribSet ){
						if(!attribSet.isEmpty()) {
							
							finalKey.clear();
							finalValue.clear();
							finalKey.set( custo);
							finalValue.set(attributes);
							context.write(finalKey, finalValue);
						} else{
							finalKey.clear();
							finalValue.clear();					
							finalKey.set(attributes);
							context.write(finalKey, EMPTY_Text);	
						}
						
					}
				}
				else {
					finalKey.clear();
					finalValue.clear();					
					finalKey.set(custo);
					context.write(finalKey, EMPTY_Text);
					
					
				}
			}
			
	};

}
