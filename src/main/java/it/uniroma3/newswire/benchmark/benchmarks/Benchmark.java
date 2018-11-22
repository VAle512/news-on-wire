/**
 * 
 */
package it.uniroma3.newswire.benchmark.benchmarks;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.*;
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
	
	protected static Logger logger = Logger.getLogger(Benchmark.class);
	protected static SQLContext sqlContext;
	
	protected String resultsTable;
	protected String database;
	
	/**
	 * Initializes the context.
	 */
	@SuppressWarnings("deprecation")
	protected static void init() {
		JavaSparkContext jsc = SparkLoader.getInstance().getContext();
		sqlContext = new SQLContext(jsc);
	}
	
	/**
	 * Loads up the data from the DB.
	 * @return a {@link JavaRDD} of {@link Document}.
	 */
	public JavaRDD<Document> loadData() {		
		String url = DAO.DB_URL.replace(DB_NAME_PLACEHOLDER, this.database);
		
		JavaRDD<Document> readResult =  sqlContext.read()
			  	   								  .format("jdbc")
			  	   								  .option("url", url)
			  	   								  .option("driver", DAO.JDBC_DRIVER)
			  	   								  .option("dbtable", LINK_OCCURRENCES)
			  	   								  .option("user", DAO.USER)
			  	   								  .option("password", DAO.PASS)
			  	   								  .load()
			  	   								  .toJavaRDD()
			  	   								  .map(row -> new Document().append(id.name(), 				row.getInt(id.ordinal()))
			  	   										                    .append(link.name(), 			row.getString(link.ordinal()))
			  	   										                    .append(referringPage.name(), 	row.getString(referringPage.ordinal()))
			  	   										                    .append(relative.name(), 	row.getString(relative.ordinal()))
			  	   										                    .append(xpath.name(), 			row.getString(xpath.ordinal()))
			  	   										                    .append(snapshot.name(), 		row.getInt(snapshot.ordinal()))
			  	   										                    .append(date.name(), 			row.getTimestamp(date.ordinal())));
			
		return readResult;
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
	
	
	public void persist(JavaPairRDD<String, Double> results, String tableName) {
		results.foreachPartition(partitionRdd -> {
				Connection connection = DAOPool.getInstance().getDAO(this.database).getConnection();
				DAOPool.getInstance().getDAO(this.database).insertAnalysisResult(connection, tableName, partitionRdd);

				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage());
				}
		});
		logger.info("Results have been correctly saved to the DB.");
	}
	
	/**
	 * If you have X benchmark, this will return XBenchmark.
	 * 
	 * @return the canonical name of the benchmark. 
	 * 
	 * It is used mainly for logging and table naming into the database.
	 */
	public abstract String getCanonicalBenchmarkName();
	
	/**
	 * If you have X benchmark, this will return X.
	 * 
	 * @return the simple name of the benchmark. 
	 * 
	 * It is used mainly for logging and table naming into the database.
	 */
	public abstract String getBenchmarkSimpleName();
		
}
