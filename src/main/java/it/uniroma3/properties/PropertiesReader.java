package it.uniroma3.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesReader {
	/* BATCH ANALYSIS */
	public static final String BATCH_TIMEUNIT = "batch.timeunit";
	public static final String BATCH_TIME_TO_WAIT = "batch.timetowait";
	
	/* MONGO DB */
	public static final String MONGODB_HOST_ADDRESS = "mongodb.host.address";
	public static final String MONGODB_HOST_PORT = "mongodb.host.port";
	public static final String MONGODB_DB_NAME = "mongodb.dbName";
	public static final String MONGODB_INPUT_COLLECTION = "mongodb.inputCollection";
	public static final String MONGODB_RESULTS_COLLECTION = "mongodb.resultsCollection";
	public static final String MONGODB_COUNTER_COLLECTION = "mongodb.counterCollection";
	public static final String MONGODB_COUNTER_ID = "mongodb.counterID";
	public static final String MONGODB_RANKS_COLLECTION = "mongodb.ranksCollection";
	
	/* SPARK */
	public static final String SPARK_APP_NAME = "spark.appName";
	public static final String SPARK_MASTER = "spark.master";
	public static final String SPARK_LOG_LEVEL = "spark.logLevel";
	public static final String SPARK_REPARTITION_LEVEL = "spark.repartition.level";
	
	/* KAFKA */
	public static final String KAFKA_BOOTSRAP_SERVERS = "kafka.bootstrap.servers";
	public static final String KAFKA_KEY_SERIALIZER = "kafka.key.serializer";
	public static final String KAFKA_KEY_DESERIALIZER = "kafka.key.deserializer";
	public static final String KAFKA_VALUE_SERIALIZER = "kafka.value.serializer";
	public static final String KAFKA_VALUE_DESERIALIZER = "kafka.value.deserializer";
	public static final String KAFKA_GROUP_ID = "kafka.group.id";
	public static final String KAFKA_TOPIC = "kafka.topic";
	
	/*GATHERER */
	public static final String GATHERER_TIMEUNIT = "gatherer.timeunit";
	public static final String GATHERER_TIME_TO_WAIT = "gatherer.timetowait";
	public static final String GATHERER_ENTRY_POINT = "gatherer.entryPoint";
	public static final String GATHERER_EXCLUDE_LIST = "gatherer.excludeList";
	public static final String GATHERER_TIMEOUT_KILL = "gatherer.timeoutKill";
	public static final String GATHERER_DEPTH_LEVEL = "gatherer.depthLevel";
	public static final String GATHERER_STABILITY_THRESHOLD = "gatherer.stabilityThreshold";
	public static final String GATHERER_DEPTH_INCREMENT_THRESHOLD = "gatherer.depth.incrementThreshold";
	public static final String GATHERER_DEPTH_DECREMENT_THRESHOLD = "gatherer.depth.decrementThreshold";
	public static final String GATHERER_DEPTH_STEPSIZE = "gatherer.depth.stepsize";
	public static final String GATHERER_DOMAIN = "gatherer.domain";
	public static final String GATHERER_WRITE_ON_FILE = "gatherer.writeonFile";
	
	/* PAGE RANK */
	public static final String PAGE_RANK_GRAPH_GENERATOR_FILE = "pagerank.graphGenerator.file";
	public static final String PAGE_RANK_GRAPH_GENERATOR_DEPTH = "pagerank.graphGenerator.depth";
	public static final String PAGE_RANK_ITERATIONS = "pagerank.iterations";

	
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	private static PropertiesReader instance;
	private Properties properties;
	
	public PropertiesReader() {
		this.properties = new Properties();	
		try {
			this.properties.load(new FileInputStream(new File("./resources/application.properties")));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}		
	}
	
	public static PropertiesReader getInstance() {
		return (instance == null) ? (instance = new PropertiesReader()) : instance;
	}
	
	public String getProperty(String key) {
		return this.properties.getProperty(key);
	}
	
	public void setProperty(String key, String value) {
		this.properties.setProperty(key, value);
	}
	
}
