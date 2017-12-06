package com.sourabh.wordcount.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * This class takes input of tuples of stream from WordCounterBolt,
 * And when a Bolt goes for shutdown, it gives a report of words and
 * their coressponding counts.
 * @author sourabhsh
 */
public class ReportingBolt extends BaseRichBolt{

	private HashMap<String, Long> reportCounts = null;
	
	/**
	 * Initialize the HashMap instance.
	 */
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.reportCounts = new HashMap<String, Long>();
	}
	
	/**
	 * It recieves the yuples of stream for
	 * word and its count.
	 * then stores in HashMap
	 * 
	 */
	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.reportCounts.put(word, count);
	}

	/**
	 * Here, we are not emitting anything
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything
	}
	
	/**
	 * This method is defined in IBolt interface,
	 * Storm calls it when a bolt goes for shutdown.
	 * 
	 * It is used for releasing resources, like open
	 * files and database connections.
	 * 
	 * Here, we are using it to putputs our final counts 
	 * when the topology shuts down.
	 */
	public void cleanup() {
		System.out.println("***********Output************");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.reportCounts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.reportCounts.get(key));
		}
		System.out.println("*****************************");
	}

}
