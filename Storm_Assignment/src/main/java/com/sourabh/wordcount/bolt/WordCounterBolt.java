package com.sourabh.wordcount.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * This bolt takes input tuples of stream from
 * SentenceSplitterBolt and gives output to ReportingBolt.
 * @author sourabhsh
 */
public class WordCounterBolt extends BaseRichBolt{

	private OutputCollector countBoltCollector;
	
	private HashMap<String, Long> wordCounts = null;
	
	/**
	 * In this method, we are saving a refernce of OutpuCollector
	 * to instance variable.
	 * And, initializing an instance of HashMap.
	 * This instance will stores the words and their corresponding counts
	 * Reason to use HashMap is serialization.
	 */
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.countBoltCollector = collector;
		this.wordCounts = new HashMap<String, Long>();
	}

	/**
	 * In this method, we look up the count for the word recieved,
	 * increment and store the count.
	 * Then emit a new tuple consisting of word and it's current 
	 * count. 
	 * 
	 */
	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.wordCounts.get(word);
		if(count == null){
			count = 0L;
		}
		count++;
		this.wordCounts.put(word, count);
		this.countBoltCollector.emit(new Values(word, count));
	}

	/**
	 * Here, it declares a stream of tuples that will contain
	 * both the word recieved and it's corresponding count.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}
