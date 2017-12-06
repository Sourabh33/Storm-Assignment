package com.sourabh.wordcount.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.sourabh.wordcount.bolt.ReportingBolt;
import com.sourabh.wordcount.bolt.SentenceSplitterBolt;
import com.sourabh.wordcount.bolt.WordCounterBolt;
import com.sourabh.wordcount.spout.SentenceListenerSpout;
import com.sourabh.wordcount.util.Utils;

public class WordCountTopology {

	public static void main(String[] args) {
		
		// This provides an API for defining the data flow between 
		// components in a topology 
		TopologyBuilder builder = new TopologyBuilder();

		//Initializing the spout and bolts.
		SentenceListenerSpout spout = new SentenceListenerSpout();
		SentenceSplitterBolt splitBolt = new SentenceSplitterBolt();
		WordCounterBolt countBolt = new WordCounterBolt();
		ReportingBolt reportBolt = new ReportingBolt();
		
		/**
		 * Registering the SentenceListenerSpout with a uniqueID
		 */
		builder.setSpout("sentence-spout", spout);

		// SentenceSpout --> SplitSentenceBolt
		
		/**
		 * Registering SentenceSplitterBolt by using setBolt method
		 * shuffleGrouping method : it tells Storm to shuffle tuples
		 * 							emitted by SentenceListenerSpout and distribute
		 * 							them evenly among instance of SentenceSplitterBolt.
		 */
		builder.setBolt("split-bolt", splitBolt)
				.shuffleGrouping("sentence-spout");

		// SplitSentenceBolt --> WordCountBolt
		
		/**
		 * Here this establishes connection between the SentenceSplitterBolt
		 * and WordCounterBolt.
		 * 
		 * fieldGrouping method : it ensure that all yuples containing the same
		 * "word" value get routed to the same WordCounterBolt.
		 */
		builder.setBolt("count-bolt", countBolt)
				.fieldsGrouping("split-bolt", new Fields("word"));

		// WordCountBolt --> ReportBolt

		/**
		 * Here it defined to route the data flow of stream of tuples emitted by WordCounterBolt
		 * directs to ReportingBolt.
		 * 
		 * globalGrouping method : This helps to provide the behaviour.
		 */
		builder.setBolt("report-bolt", reportBolt)
				.globalGrouping("count-bolt");
	
		
		Config config = new Config();
		
		/**
		 * LocalCLuster class to simulate a Storm cluster in our local development
		 * environment.
		 * This gives a way to develop and test Storm applications and help to run 
		 * Storm topologies in an IDE.
		 */
		LocalCluster cluster = new LocalCluster();
		
		/**
		 * This method submit the topology to the LocalCluster intsance,
		 * it takes three parameters.
		 * topology name
		 * storm config
		 * Topology object returned by TopologyBuilder class createTopology method.
		 *  
		 */
		cluster.submitTopology("word-count-topology", config, builder.createTopology());
		
		// waiting for 100 seconds while Toplogy is running
		Utils.waitForSeconds(100);
		
		// To undeploy the topology
		cluster.killTopology("word-count-topology");
		
		// shut down the cluster
		cluster.shutdown();
	}
}
