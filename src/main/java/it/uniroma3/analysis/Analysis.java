/**
 * 
 */
package it.uniroma3.analysis;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.spark.SparkLoader;

/**
 * @author Luigi D'Onofrio
 *
 */
public abstract class Analysis {
	protected static final Logger logger = Logger.getLogger(Analysis.class);
	protected static SQLContext sqlContext;
		
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
	 * @param tableName is the name of the table we want to load data of.
	 * @return a {@link JavaRDD} of {@link Row}.
	 */
	public static JavaRDD<Row> loadData(String tableName) {
		return sqlContext.read()
			  	   .format("jdbc")
			  	   .option("url", MySQLRepositoryDAO.DB_URL)
			  	   .option("driver", MySQLRepositoryDAO.JDBC_DRIVER)
			  	   .option("dbtable", tableName)
			  	   .option("user", MySQLRepositoryDAO.USER)
			  	   .option("password", MySQLRepositoryDAO.PASS)
			  	   .load()
			  	   .toJavaRDD();
	}
	
	/**
	 * Does the analysis.
	 * @param persist should be true if you wanna persist results on the DB, false otherwise.
	 * @return a {@link JavaRDD} of {@link Document} like:<br>
	 * <li>
	 * 	<ul><b>url</b>: the URL</ul>
	 * 	<ul><b>score</b>: the score for this analysis</ul>
	 * </li>
	 */
	public abstract JavaRDD<Document> analyze(boolean persist);
	
	
}
