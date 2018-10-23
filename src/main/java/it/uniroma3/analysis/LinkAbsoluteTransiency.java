package it.uniroma3.analysis;

import static it.uniroma3.persistence.MySQLRepositoryDAO.URLS_TABLE_NAME;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import scala.Tuple3;

/**
 * This class is intended for calculating the results of the Absolute Transiency Analysis.
 * This analysis is based on the simple principle that the more a URLs is crawled (into snapshots) the more it is non-transient so it lasts for a long time. 
 * @author Luigi D'Onofrio
 *
 */
public class LinkAbsoluteTransiency extends Analysis {
	
	/**
	 * Constructor.
	 */
	public LinkAbsoluteTransiency() {
		init();
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	public JavaRDD<Document> analyze(boolean persist) {
		/* Erase previous IPS Data */
		if(persist)
			MySQLRepositoryDAO.getInstance().createAbsoluteTransiencyTable();
		
		JavaRDD<Document> rdd =  loadData(URLS_TABLE_NAME)
									  	   /* Let's map the Row in a Document made of the following fields.
									  	    * It's just easier. Nothing more.
									  	    */
									  	   .map(row -> new Document().append("id", row.getInt(0))
											                    	 .append("url", row.getString(1))
											                    	 .append("snapshot", row.getInt(2))
											                    	 .append("date", row.getTimestamp(3)));

		/* Retrieve the current snapshot counter */
		int latestSnapshot = MySQLRepositoryDAO.getInstance().getCurrentSequence();
		
		/* Let's map into (id, url, snapshot) */
		JavaRDD<Document> result = rdd.map(doc -> new Tuple3<>(Integer.parseInt(doc.get("id").toString()), 
																				doc.get("url").toString(), 
																				Integer.parseInt(doc.get("snapshot").toString())))
									  /* Group by url so we have (url, (s1, s2, s3...)) 
									   * (Actually there's more but that's the basic idea.  
									   */
									  .groupBy(tuple -> tuple._2())
									  /* divide the number of snapshots a url is crawled in by the total number of snapshots taken. */
									  .map(group -> new Tuple3<>(Iterables.asList(group._2).get(0)._1().intValue(), 
											  									  group._1 , 
											  									  (double) Iterables.asList(group._2).size() / (double) latestSnapshot))
									  /* Map all in documents to re-give semantic to the fields. */
									  .map(tuple -> new Document().append("id", tuple._1()).append("url",tuple._2()).append("score", tuple._3()));
		
	   /* Persisting results into the DB. */
	   if(persist) {
		   result.foreachPartition(partitionRdd -> {
			   					Connection connection = MySQLRepositoryDAO.getConnection();
			   					MySQLRepositoryDAO.getInstance().insertStabilityResults(connection, partitionRdd);
			
			   					try {
			   						connection.close();
			   					} catch (SQLException e) {
			   						logger.error(e.getMessage());
			   					}
		   });
		   logger.info("Results have been correctly saved to the DB.");
	   }
	   
		return result;
	}
}
