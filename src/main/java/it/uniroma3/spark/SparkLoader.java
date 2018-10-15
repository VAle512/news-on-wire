package it.uniroma3.spark;

import static it.uniroma3.properties.PropertiesReader.MONGODB_DB_NAME;
import static it.uniroma3.properties.PropertiesReader.MONGODB_HOST_ADDRESS;
import static it.uniroma3.properties.PropertiesReader.MONGODB_INPUT_COLLECTION;
import static it.uniroma3.properties.PropertiesReader.MONGODB_RESULTS_COLLECTION;
import static it.uniroma3.properties.PropertiesReader.SPARK_APP_NAME;
import static it.uniroma3.properties.PropertiesReader.SPARK_MASTER;
import static it.uniroma3.properties.PropertiesReader.SPARK_NEO4J_URI;
import static it.uniroma3.properties.PropertiesReader.SPARK_NEO4J_USER;
import static it.uniroma3.properties.PropertiesReader.SPARK_NEO4J_PASSWORD;
//import static it.uniroma3.properties.PropertiesReader.SPARK_LOG_LEVEL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import it.uniroma3.properties.PropertiesReader;

public class SparkLoader {
	private static SparkLoader instance;
	private JavaSparkContext jsc;
	private static PropertiesReader propsReader = PropertiesReader.getInstance();

	private SparkLoader() {
		final String mongoDBHost = propsReader.getProperty(MONGODB_HOST_ADDRESS);
		final String mongoDBName = propsReader.getProperty(MONGODB_DB_NAME);
		final String mongoDBInputCollection = propsReader.getProperty(MONGODB_INPUT_COLLECTION);
		final String mongoDBResultsCollection = propsReader.getProperty(MONGODB_RESULTS_COLLECTION);
		
		String mongodbSparkInput = "mongodb://" + mongoDBHost +"/" + mongoDBName + "." + mongoDBInputCollection;
		String mongodbSparkOutput = "mongodb://" + mongoDBHost +"/" + mongoDBName + "." + mongoDBResultsCollection;
		
		SparkConf sparkConf = new SparkConf().setAppName(propsReader.getProperty(SPARK_APP_NAME))
											 .setMaster(propsReader.getProperty(SPARK_MASTER))
											 .set("spark.mongodb.input.uri", mongodbSparkInput)
											 .set("spark.mongodb.output.uri", mongodbSparkOutput)
											 .set(SPARK_NEO4J_URI, propsReader.getProperty(SPARK_NEO4J_URI))
											 .set(SPARK_NEO4J_USER, propsReader.getProperty(SPARK_NEO4J_USER))
											 .set(SPARK_NEO4J_PASSWORD, propsReader.getProperty(SPARK_NEO4J_PASSWORD));
		
		this.jsc = new JavaSparkContext(sparkConf);
	
	}
	
	public JavaMongoRDD<Document> loadDataFromMongo(){
		return MongoSpark.load(jsc);
	}
	
	public JavaSparkContext getContext() {
		return this.jsc;
	}
	
	public void close() {
		this.jsc.stop();
		this.jsc.close();
	}
	
	public static SparkLoader getInstance() {
		return (instance==null) ? (instance = new SparkLoader()) : instance;
	}

}
