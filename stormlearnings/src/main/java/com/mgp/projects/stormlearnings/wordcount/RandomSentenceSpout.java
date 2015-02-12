/**
 * 
 */
package com.mgp.projects.stormlearnings.wordcount;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author pradeepm.gireesha
 * 
 */

// Spout gets started once and will be listening to the Stream of data
public class RandomSentenceSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	Random _rand;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		_collector = collector;
		_rand = new Random();

	}

	public void nextTuple() {
		Utils.sleep(100);
		String[] sentences = new String[] { "Twinkle twinkle Little Star ",
				"How I wonder what you are", "Up above the world so high",
				"Like a diamond in the sky" };

		String sentence = sentences[_rand.nextInt(sentences.length)];
		_collector.emit(new Values(sentence));
//		System.out.println(sentence);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
