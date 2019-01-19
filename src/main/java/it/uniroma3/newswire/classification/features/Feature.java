/**
 * 
 */
package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.date;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.depth;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.file;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.relative;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL_PLACEHOLDER;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.spark.SparkLoader;
import scala.Tuple2;

/**
 * @author Luigi D'Onofrio
 *
 */
public abstract class Feature implements Serializable {
	private static final long serialVersionUID = 8907698548835775816L;

	protected static PropertiesReader propsReader = PropertiesReader.getInstance();
	
	protected static final String LINK_OCCURRENCES = DAO.LINK_OCCURRENCES_TABLE;
	protected static final String DB_NAME_PLACEHOLDER = propsReader.getProperty(MYSQL_DB_URL_PLACEHOLDER);
	
	protected static final Logger logger = Logger.getLogger(Feature.class);
	protected SQLContext sqlContext;
	protected String resultsTable;
	protected String database;
	protected JavaRDD<Document> cachedData;
	
	/**
	 * Constructor.
	 * @param dbName is the database of the website we are executing the benchmark for.
	 */
	public Feature(String dbName) {
		this.resultsTable = getBenchmarkSimpleName(); //Not the benchmark table but the results one.
		this.database = dbName;
		init();
	}
	/**
	 * Initializes the context.
	 */
	@SuppressWarnings("deprecation")
	protected void init() {
		JavaSparkContext jsc = SparkLoader.getInstance().getContext();
		sqlContext = new SQLContext(jsc);
	}
	
	/**
	 * Loads up the data from the DB. It caches the data for future uses.
	 * @return a {@link JavaRDD} of {@link Document}.
	 */
	public JavaRDD<Document> loadData() {		
		String url = DAO.DB_URL.replace(DB_NAME_PLACEHOLDER, this.database);
		
		return DataLoader.getInstance().loadData(this.database);
	}
	
	/**
	 * Loads up the data from the DB. It caches the data for future uses.
	 * @return a {@link JavaRDD} of {@link Document}.
	 */
	public JavaPairRDD<String, Double> loadCachedData() {		
		String url = DAO.DB_URL.replace(DB_NAME_PLACEHOLDER, this.database);
		
		try {
			return sqlContext.read()
						 	.format("jdbc")
						 	.option("url", url)
							.option("driver", DAO.JDBC_DRIVER)
							.option("dbtable", this.getClass().getSimpleName())
							.option("user", DAO.USER)
							.option("password", DAO.PASS)
							.load()
							.toJavaRDD()
							.mapToPair(row -> new Tuple2<>(row.getString(0), row.getDouble(1)))
							.cache();
		}catch (Exception e) {
			return null;
		}
		
	}
		
	/**
	 * Does the analysis.
	 * @param persist should be true if you want to persist results on the DB, false otherwise.
	 * @return a {@link JavaPairRDD} of:<br>
	 * <li>
	 * 	<ul><b>URL</b>: the URL</ul>
	 * 	<ul><b>score</b>: the score for this analysis</ul>
	 * </li>
	 */
	public abstract JavaPairRDD<String, Double> calculate(boolean persist, int untilSnapshot);
	
	/**
	 * If you have X benchmark, this will return XBenchmark.
	 * 
	 * @return the canonical name of the benchmark. 
	 * 
	 * It is used mainly for logging and table naming into the database.
	 */
	public String getCanonicalBenchmarkName() {
		return this.getClass().getSimpleName() + "Benchmark";
	}
	
	/**
	 * If you have X benchmark, this will return X.
	 * 
	 * @return the simple name of the benchmark. 
	 * 
	 * It is used mainly for logging and table naming into the database.
	 */
	public String getBenchmarkSimpleName() {
		return this.getClass().getSimpleName();
	}
	
	/**
	 * Saves results to the relative results table.
	 * @param results is the RDD of (url, score) we want to persist.
	 */
	public void persist(JavaPairRDD<String, Double> results) {
		results.foreachPartition(partitionRdd -> {	
				DAOPool.getInstance().getDAO(this.database).insertAnalysisResult(this.resultsTable, partitionRdd);
		});
		logger.info("Results have been correctly saved to the DB.");
	}
	
	/**
	 * If @param persistResults is true, it deletes all the previous benchmarked data, nothing otherwise. 
	 * @param persistResults
	 */
	protected void erasePreviousBenchmarkData(boolean persistResults) {
		/* Erase previous Link Occurrence Dinamicity Data */
		if(persistResults) {	
			if(this.getAssociatedDAO().checkTableExists(this.resultsTable))
				this.getAssociatedDAO().cleanTable(this.resultsTable);
			else
				this.getAssociatedDAO().createAnalysisTable(this.resultsTable);
		}	
	}
	
	/**
	 * Returns the DAO associated with this benchmark.
	 * @return the DAO.
	 */
	public DAO getAssociatedDAO() {
		return DAOPool.getInstance().getDAO(this.database);
	}
	
	
	/**
	 * @param score
	 * @param threshold
	 * @return true if score is thresholded to threshold, false otherwise.
	 */
	public abstract boolean isThresholded(Double score, Double threshold);
	
	protected void log(Level level, String message) {
		String toAppend = "[" + this.database + "] -- " + this.getClass().getSimpleName() +" -- ";
		logger.log(level, toAppend + " " + message);
	}
		
}
