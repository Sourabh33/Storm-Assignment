package com.sourabh.wordcount.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.sourabh.wordcount.model.Sentence;
import com.sourabh.wordcount.util.Utils;

/**
 * BaseRichSpout class implements the ISpout and 
 * IComponent interfaces
 * 
 * @author sourabhsh
 *
 */
public class SentenceListenerSpout extends BaseRichSpout{

	private SpoutOutputCollector spoutCollector;
	
	private Sentence sentences = new Sentence();
	
	private int index = 0;
	
	/**
	 * This method is defined in ISpout interface
	 * It is called by storm when a spout is initialized
	 * 
	 * Takes 3 parameters as input;
	 * Map : a map contains storm congiguration
	 * TopologyContext : provides info about spout in a topology
	 * SpoutOutputCOllector : provides method for emitting tuples
	 *  
	 * Here, we are storing the reference of SpoutOutputCollector
	 * to our instance variable.
	 */
	@Override
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.spoutCollector = collector;
	}
	
	/**
	 * 
	 * This method represent core implementation logic
	 * for a spout.
	 * 
	 * Here, we emit the sentence at current index, and 
	 * increment the index.
	 * 
	 */
	@Override
	public void nextTuple() {
		this.spoutCollector.emit(new Values(sentences.getSentences()[index]));
		index++;
		if (index >= sentences.getSentences().length) {
			index = 0;
		}
		Utils.waitForMillis(1);
		
	}

	/**
	 * 
	 * This method is defined in IComponent interface.
	 * It must be implement by every Spout in Storm.
	 * 
	 * It tells Strom what stream the Spout will emit,
	 * and also about the fields each stream's tuples will contain.
	 * 
	 * Here, we are emitting a single stream of tuples
	 * containing a single field, i.e.
	 * Fields("sentence")
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
