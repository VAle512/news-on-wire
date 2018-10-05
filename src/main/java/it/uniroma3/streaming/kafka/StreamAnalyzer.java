package it.uniroma3.streaming.kafka;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import it.uniroma3.persistence.RepositoryDAO;

public class StreamAnalyzer {
	private static StreamAnalyzer instance; 
	private JavaInputDStream<ConsumerRecord<String, String>> messages;
	private static final Pattern url2Cluster = Pattern.compile("(.*)\\$\\$(\\d+)");
	private static final Logger logger = Logger.getLogger(StreamAnalyzer.class);
	private static AtomicInteger foundCounter = new AtomicInteger(0);
	private static AtomicInteger newFoundCounter = new AtomicInteger(0);
	
	private StreamAnalyzer() {}
	
	public StreamAnalyzer attach(JavaInputDStream<ConsumerRecord<String, String>> messages) {
		if(this.messages == null)
			this.messages = messages;
		else
			logger.error("Stream already attached! Ignoring");
		return this;	
	}
	
	public void run() {
		messages.map(x -> x.value())
				.foreachRDD(x -> { 
					long count = x.count();
					x.foreach(y -> {
						Matcher matcher = url2Cluster.matcher(y);
						if(matcher.find()) {
							String url = matcher.group(1);
							String snaphotCounter = matcher.group(2);
							if(!RepositoryDAO.getInstance().isStored(url, snaphotCounter)) {
								foundCounter.incrementAndGet();
								newFoundCounter.incrementAndGet();
							}else if(!RepositoryDAO.getInstance().isStable(url)) {
								foundCounter.incrementAndGet();
							}
								
							
							RepositoryDAO.getInstance().insertURLWithSnapshot(url, snaphotCounter);
						}
						
					});
					if(count!=0)
						logger.info("Stored " + count + " batch.");
					//else
						//logger.info("Wating for batch to be received");
				}
				);
	}
	
	public double getCoverage() {
		if(this.messages != null) {
			logger.info("New: " + newFoundCounter.get());
			logger.info("Found: " + foundCounter.get());
			return ((double) newFoundCounter.get()) / ((double) foundCounter.get());
		} else {
			logger.error("Cannot calculate coverage! Stream not attached. Returning -1.");
			return -1.;
		}
	}
	
	public void resetCoverage() {
		if(this.messages != null) {
			foundCounter.set(0);
			newFoundCounter.set(0);
		}
	}
	
	public static StreamAnalyzer getInstance() {
		return (instance == null) ? (instance = new StreamAnalyzer()) : instance;
	}
	
}
