/**
 * 
 */
package com.mgp.projects.stormlearnings.wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author pradeepm.gireesha
 * 
 */
public class WordCountTopology {

	// when BaseBasicBolt is used there will be no hold on the number of times it gets executed by the concurrent threads
	
	public static class SplitSentence extends BaseBasicBolt{//ShellBolt implements IRichBolt {

//		public SplitSentence() {
//			super("python", "splitsentence.py");
//
//		}
		/**
		 * class SplitSentenceBolt(storm.BasicBolt):
def process(self, tup):
words = tup.values[0].split(" ")
for word in words:
storm.emit([word])
SplitSentenceBolt().run()
		 */
		public void execute(Tuple input, BasicOutputCollector collector) {
			String []words= input.toString().split(" ");
			for(String word : words) {
//				System.out.println("word => "+ word);
			collector.emit(new Values(word));

		}
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));

		}

		public Map<String, Object> getComponentConfiguration() {
			return null;
		}

	}

	public static class WordCount extends BaseBasicBolt {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		public void execute(Tuple input, BasicOutputCollector collector) {
			String word = input.getString(0);
			Integer count = counts.get(word);
			if (count == null)
				count = 0;
			count++;
			counts.put(word, count);
			System.out.println(word);
			collector.emit(new Values(word, count));

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {

			declarer.declare(new Fields("word", "count"));

		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new RandomSentenceSpout(), 1);
		builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping(
				"spout");
		builder.setBolt("count", new WordCount(), 2).fieldsGrouping("split",
				new Fields("word"));

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());

		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();

		}

	}

}
