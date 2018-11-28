/**
 * 
 */
package it.uniroma3.newswire.benchmark.benchmarks;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.date;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.relative;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL_PLACEHOLDER;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.spark.SparkLoader;

/**
 * @author Luigi D'Onofrio
 *
 */
public abstract class Benchmark implements Serializable {
	private static final long serialVersionUID = 8907698548835775816L;

	protected static PropertiesReader propsReader = PropertiesReader.getInstance();
	
	protected static final String LINK_OCCURRENCES = DAO.LINK_OCCURRENCES_TABLE;
	protected static final String DB_NAME_PLACEHOLDER = propsReader.getProperty(MYSQL_DB_URL_PLACEHOLDER);
	
	protected Logger logger = Logger.getLogger(Benchmark.class);
	protected SQLContext sqlContext;
	protected String resultsTable;
	protected String database;
	protected JavaRDD<Document> cachedData;
	protected DAO dao;
	
	/**
	 * Constructor.
	 * @param dbName is the database of the website we are executing the benchmark for.
	 */
	public Benchmark(String dbName) {
		this.resultsTable = getBenchmarkSimpleName(); //Not the benchmark table but the results one.
		this.database = dbName;
		this.dao = DAOPool.getInstance().getDAO(dbName);

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
		
		if(cachedData == null)
			this.cachedData =  sqlContext.read()
			  	   						 	.format("jdbc")
			  	   						 	.option("url", url)
			  	   							.option("driver", DAO.JDBC_DRIVER)
			  	   							.option("dbtable", LINK_OCCURRENCES)
			  	   							.option("user", DAO.USER)
			  	   							.option("password", DAO.PASS)
			  	   							.load()
			  	   							.toJavaRDD()
			  	   							.map(row -> new Document().append(id.name(), 				row.getInt(id.ordinal()))
			  	   													  .append(link.name(), 				row.getString(link.ordinal()))
			  	   													  .append(referringPage.name(), 	row.getString(referringPage.ordinal()))
			  	   										              .append(relative.name(), 			row.getString(relative.ordinal()))
			  	   										              .append(xpath.name(), 			row.getString(xpath.ordinal()))
			  	   										              .append(snapshot.name(), 			row.getInt(snapshot.ordinal()))
			  	   										              .append(date.name(), 				row.getTimestamp(date.ordinal())))
			  	   							.cache();
		
		return this.cachedData;
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
	public abstract JavaPairRDD<String, Double> analyze(boolean persist, int untilSnapshot);
	
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
				Connection connection = DAOPool.getInstance().getDAO(this.database).getConnection();
				DAOPool.getInstance().getDAO(this.database).insertAnalysisResult(connection, this.resultsTable, partitionRdd);

				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage());
				}
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
			if(this.dao.checkTableExists(this.resultsTable))
				this.dao.cleanTable(this.resultsTable);
			else
				this.dao.createAnalysisTable(this.resultsTable);
		}	
	}
		
}
