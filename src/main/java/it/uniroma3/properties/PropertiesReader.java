package it.uniroma3.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesReader {
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
	public static final String SPARK_NEO4J_URI = "spark.neo4j.bolt.uri";
	public static final String SPARK_NEO4J_USER = "spark.neo4j.bolt.user";
	public static final String SPARK_NEO4J_PASSWORD = "spark.neo4j.bolt.password";

	/* PAGE RANK */
	public static final String PAGE_RANK_GRAPH_GENERATOR_FILE = "pagerank.graphGenerator.file";
	public static final String PAGE_RANK_GRAPH_GENERATOR_DEPTH = "pagerank.graphGenerator.depth";
	public static final String PAGE_RANK_ITERATIONS = "pagerank.iterations";
	
	/* CRAWLER */
	public static final String CRAWLER_TIMEUNIT = "crawler.timeunit";
	public static final String CRAWLER_TIME_TO_WAIT = "crawler.timetowait";
	public static final String CRAWLER_DEPTH = "crawler.depth";
	public static final String CRAWLER_NUM_CRAWLERS = "crawler.numCrawlers";
	public static final String CRAWLER_STORAGE = "crawler.storageFolder";
	public static final String CRAWLER_SEEDS = "crawler.seeds";
	public static final String CRAWLER_EXCLUDE_LIST = "crawler.excludeList";
	public static final String CRAWLER_DOMAIN = "crawler.domain";
	public static final String CRAWLER_WRITE_ON_FILE = "crawler.writeonFile";
	public static final String CRAWLER_FILE_TO_WRITE_ON = "crawler.writeon";
	
	/* NEO4J */
	public static final String NEO4J_URI = "neo4j.uri";
	public static final String NEO4J_USER = "neo4j.user";
	public static final String NEO4J_PASSWORD = "neo4j.password";

	
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
