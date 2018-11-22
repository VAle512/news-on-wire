package it.uniroma3.newswire.spark;

import static it.uniroma3.newswire.properties.PropertiesReader.SPARK_APP_NAME;
import static it.uniroma3.newswire.properties.PropertiesReader.SPARK_MASTER;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import it.uniroma3.newswire.properties.PropertiesReader;

/**
 * Class made to make easier the startup of a Spark Context and its sharing between Spark jobs.
 * @author Luigi D'Onofrio
 */
public class SparkLoader {
	private static SparkLoader instance;
	private JavaSparkContext jsc;
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();

	/**
	 * Constructor.
	 */
	private SparkLoader() {
		SparkConf sparkConf = new SparkConf().setAppName(propsReader.getProperty(SPARK_APP_NAME))
											 .setMaster(propsReader.getProperty(SPARK_MASTER));
		this.jsc = new JavaSparkContext(sparkConf);
	}
	
	/**
	 * @return the {@link JavaSparkContext} associated to this instance;
	 */
	public JavaSparkContext getContext() {
		return this.jsc;
	}
	
	/**
	 * Stops and closes the {@link JavaSparkContext} to release all the involved resources.
	 */
	public void close() {
		this.jsc.stop();
		this.jsc.close();
	}
	
	/**
	 * @return a reference the the Singleton instance representing this object.
	 */
	public static SparkLoader getInstance() {
		return (instance == null) ? (instance = new SparkLoader()) : instance;
	}

}
