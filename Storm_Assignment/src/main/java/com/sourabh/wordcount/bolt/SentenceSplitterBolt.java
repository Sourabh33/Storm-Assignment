package com.sourabh.wordcount.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * BaseRichBolt implements IComponent and IBolt interface
 * It takes input of tuples from SentenceListenerSpout
 * and give output tuples to WordCounterBolt
 * 
 * @author sourabhsh
 */
public class SentenceSplitterBolt extends BaseRichBolt {

	private OutputCollector splitBoltCollector;
	
	/**
	 * This method is analogous to open method
	 * Defined by IBolt interface
	 * 
	 * We can prepare our resources here, such as
	 * Database cannection during bolt is initialized.
	 * 
	 * Here, we are saving the reference of OutputCollector
	 * to instance variable.
	 * 
	 */
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.splitBoltCollector = collector;
	}
	
	/**
	 * This method contains core functionality of
	 * Bolt. It is called everytime when a bolt recieves a 
	 * tuple from a stream.
	 * 
	 * Here, it looks up the value of "sentence" field of
	 * incoming tuple as a string, splits the value into individual
	 * words, and emits a new tuple for each word.
	 */
	@Override
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			this.splitBoltCollector.emit(new Values(word));
		}
	}

	/**
	 * This method declares a single stream of
	 * tuples, each containing one field,
	 * i.e. Fields("word").
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
