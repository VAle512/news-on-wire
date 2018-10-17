package it.uniroma3.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * This class models a simple properties reader. 
 * The file we have to edit is 'application.properties' located under 'resources'. 
 * @author Luigi D'Onofrio
 *
 */
public class PropertiesReader {
	/* SPARK */
	public static final String SPARK_APP_NAME = "spark.appName";
	public static final String SPARK_MASTER = "spark.master";
	public static final String SPARK_LOG_LEVEL = "spark.logLevel";
	public static final String SPARK_REPARTITION_LEVEL = "spark.repartition.level";
	public static final String SPARK_NEO4J_URI = "spark.neo4j.bolt.uri";
	public static final String SPARK_NEO4J_USER = "spark.neo4j.bolt.user";
	public static final String SPARK_NEO4J_PASSWORD = "spark.neo4j.bolt.password";
	
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
	
	/* MYSQL */
	public static final String MYSQL_JDBC_DRIVER = "mysql.jdbc.driver";  
	public static final String MYSQL_DB_URL = "mysql.uri";
	public static final String MYSQL_USER = "mysql.user";
	public static final String MYSQL_PASS = "mysql.password";
	public static final String MYSQL_URLS_TABLE_NAME = "mysql.tables.urls";
	public static final String MYSQL_STABILITY_TABLE_NAME = "mysql.tables.stability";
	public static final String MYSQL_LINKS_TABLE_NAME = "mysql.tables.links";
	public static final String MYSQL_SEQUENCE_TABLE_NAME = "mysql.tables.sequence";
	public static final String MYSQL_SEQUENCE_ID = "mysql.tables.sequence.id";
	public static final String MYSQL_IPS_TABLE_NAME = "mysql.tables.ips";
	
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	private static PropertiesReader instance;
	private Properties properties;
	
	/**
	 * Constructor.
	 */
	public PropertiesReader() {
		this.properties = new Properties();	
		try {
			this.properties.load(new FileInputStream(new File("./src/main/resources/application.properties")));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}		
	}
	
	/**
	 * Retrieves the property specified by the provided key.
	 * @param key is the key associated with the property we want to retrieve.
	 * @return the property value associated to key.
	 */
	public String getProperty(String key) {
		return this.properties.getProperty(key);
	}
	
	/**
	 * @return a reference the the Singleton instance representing this object.
	 */
	public static PropertiesReader getInstance() {
		return (instance == null) ? (instance = new PropertiesReader()) : instance;
	}
	
}
