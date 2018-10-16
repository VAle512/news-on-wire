package it.uniroma3.analysis;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.spark.SparkLoader;
import scala.Tuple3;

/**
 * This class is intended for calculating the results of the Stability Analysis.
 * This analysis is based on the simple principle that the more a URLs is crawled (into snapshots) the more it is stable. 
 * @author Luigi D'Onofrio
 *
 */
public class StabilityAnalysis {
	private static final Logger logger = Logger.getLogger(StabilityAnalysis.class);
	public static void analyze() {
		JavaSparkContext jsc = SparkLoader.getInstance().getContext();
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(jsc);
		
		JavaRDD<Document> rdd =  sqlContext.read()
									  	   .format("jdbc")
									  	   .option("url", MySQLRepositoryDAO.DB_URL)
									  	   .option("driver", MySQLRepositoryDAO.JDBC_DRIVER)
									  	   .option("dbtable", MySQLRepositoryDAO.URLS_TABLE_NAME)
									  	   .option("user", MySQLRepositoryDAO.USER)
									  	   .option("password", MySQLRepositoryDAO.PASS)
									  	   .load()
									  	   .toJavaRDD()
									  	   /* Let's map the Row in a Document made of the following fields.
									  	    * It's just easier. Nothing more.
									  	    */
									  	   .map(row -> new Document().append("id", row.getInt(0))
											                    	 .append("url", row.getString(1))
											                    	 .append("snapshot", row.getInt(2))
											                    	 .append("date", row.getTimestamp(3)));
		
		/* Retrieve the current snapshot counter */
		int latestSnapshot = MySQLRepositoryDAO.getInstance().getCurrentSequence();
		
		
		rdd.map(doc -> new Tuple3<>(Integer.parseInt(doc.get("id").toString()), 
																				doc.get("url").toString(), 
																				Integer.parseInt(doc.get("snapshot").toString())))
		   .groupBy(tuple -> tuple._2())
		   .map(group -> new Tuple3<>(Iterables.asList(group._2).get(0)._1().intValue(), 
									  group._1 , 
									  (double) Iterables.asList(group._2).size() / (double) latestSnapshot))
		   .map(tuple -> new Document().append("id", tuple._1()).append("url",tuple._2()).append("stability", tuple._3()))
		   /* Persisting results into the DB. */
		   .foreachPartition(partitionRdd -> {
			   					Connection connection = MySQLRepositoryDAO.getConnection();
			   					MySQLRepositoryDAO.getInstance().insertResults(connection, partitionRdd);
			
			   					try {
			   						connection.close();
			   					} catch (SQLException e) {
			   						logger.error(e.getMessage());
			   					}
		   });
		logger.info("Results have been correclty saved to the DB.");
	}
}
