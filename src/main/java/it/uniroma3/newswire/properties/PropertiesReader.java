package it.uniroma3.newswire.properties;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;

import java.io.File;
import java.io.FileReader;
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
	public static final String CRAWLER_SEEDS = "crawler.seedsFile";
	public static final String CRAWLER_EXCLUDE_LIST = "crawler.excludeList";
	public static final String CRAWLER_DOMAIN = "crawler.domain";
	public static final String CRAWLER_WRITE_ON_FILE = "crawler.writeonFile";
	public static final String CRAWLER_FILE_TO_WRITE_ON = "crawler.writeon";
	public static final String CRAWLER_SOCIAL_EXCLUDE_LIST = "crawler.social.excludeList";
	public static final String CRAWLER_GOLDEN_DIR = "crawler.golden.dir";
	
	/* MYSQL */
	public static final String MYSQL_RESET_ALL = "mysql.resetAll"; 
	public static final String MYSQL_JDBC_DRIVER = "mysql.jdbc.driver";  
	public static final String MYSQL_DB_URL = "mysql.uri";
	public static final String MYSQL_DB_URL_PLACEHOLDER = "mysql.uri.dbname.placeholder";
	public static final String MYSQL_USER = "mysql.user";
	public static final String MYSQL_PASS = "mysql.password";
	public static final String MYSQL_URLS_TABLE_NAME = "mysql.tables.urls";
	public static final String MYSQL_STABILITY_TABLE_NAME = "mysql.tables.stability";
	public static final String MYSQL_LINK_OCCURRENCES_TABLE_NAME = "mysql.tables.links";
	public static final String MYSQL_SEQUENCE_TABLE_NAME = "mysql.tables.sequence";
	public static final String MYSQL_SEQUENCE_ID = "mysql.tables.sequence.id";
	public static final String MYSQL_IPS_TABLE_NAME = "mysql.tables.ips";
	
	/* BENCHMARKING */
	public static final String BENCHMARK_REDO_ALL = "benchmark.redoAll";
	
	private static final String ENV_VAR_CONFIG = envConfig;
	private static final Logger logger = Logger.getLogger(PropertiesReader.class);
	private static PropertiesReader instance;
	private Properties properties;
	
	/**
	 * Constructor.
	 */
	public PropertiesReader() {
		this.properties = new Properties();	
		try {
			String configPath = System.getenv(ENV_VAR_CONFIG);
			
			File propertiesFile = new File(configPath + File.separator + "application.properties");
			this.properties.load(new FileReader(propertiesFile));
			logger.info("Properties file read correctly!");
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
