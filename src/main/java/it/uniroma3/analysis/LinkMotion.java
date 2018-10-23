package it.uniroma3.analysis;

import static it.uniroma3.persistence.MySQLRepositoryDAO.LINKS_TABLE_NAME;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;
import org.spark_project.guava.collect.Sets;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import scala.Tuple2;

/**
 * This class is intended to quantify the motion of a link 
 * in terms of how many xpath changes it counts across all the pages that reference that link.
 * @author Luigi D'Onofrio
 *
 */
public class LinkMotion extends Analysis{
	
	/**
	 * Constructor.
	 */
	public LinkMotion() {
		init();
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	@SuppressWarnings("unchecked")
	public JavaRDD<Document> analyze(boolean persist) {
		/* Erase previous Link Motion Data */
		if(persist)
			MySQLRepositoryDAO.getInstance().createLinkMotionTable();
		
		JavaRDD<Document> rdd =  loadData(LINKS_TABLE_NAME)
								  	   /* Let's map the Row in a Document made of the following fields.
								  	    * It's just easier. Nothing more.
								  	    */
								  	   .map(row -> new Document().append("id", row.getInt(0))
										                    	 .append("link", row.getString(1))
										                    	 .append("referringPage", row.getString(2))
										                    	 .append("relativeLink", row.getString(3))
								  	   							 .append("xpath", row.getString(4))
								  	   							 .append("snapshot", row.getInt(5))
								  	   							 .append("date", row.getTimestamp(6)));
		
		/* 
		 * Here we group by (link, referring page) to isolate link occurrences 
		 * of the same page but picked in different snapshots.
		 */
		JavaRDD<Document> result = rdd.mapToPair(row -> new Tuple2<>(new Tuple2<>(row.get("link").toString(), row.get("referringPage").toString()), new Tuple2<>((row.get("xpath")==null) ? null : row.get("xpath").toString(),Integer.parseInt(row.get("snapshot").toString()))))
									  /* Some XPath could be null, better to remove them. */
									  .filter(row -> row._2._1!=null)
									  .groupBy(pair -> pair._1)   
									  .map(group -> {   
										  /* Let's count how many "XPath's changes" there are for a link in the same page. */
										  int count = Sets.cartesianProduct(Sets.newHashSet(group._2), Sets.newHashSet(group._2))
										   	   		      .stream()
										   	   		      /* Here we have all the possible couples of link occurrences... */
										   	   		      .map(x -> new Tuple2<>(x.get(0), x.get(1)))
										   	   		      /* ... but we want to compare XPaths across adjacent only snapshots.
										   	   		       * e.g. (s1,s2), (s2,s3)... 
										   	   		       */
										   	   		      .filter(x ->  x._2._2._2 == x._1._2._2 + 1)
										   	   		      /* Now we set 1 if the XPath across two snapshots has changed, 0 otherwise. */
										   	   		      .mapToInt(tuple -> !tuple._1._2._1.equals(tuple._2._2._1) ? 1 : 0)
										   	   		      .sum();	   	   
								   return new Tuple2<>(group._1, count);
									   })
		   .map(tuple -> new Tuple2<>(tuple._1, tuple._2))
		   .groupBy(tuple -> tuple._1)
		   .map(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x-> x._2).sum()))
		   .map(tuple -> new Document().append("url", tuple._1._1).append("score", tuple._2.intValue()).append("referringPage", tuple._1._2));
		
		/* Persist results on the DB. 
		 * WARNING: referringPage not persisted but could be useful! 
		 */
		if(persist) {
			result.mapToPair(doc -> new Tuple2<>(doc.getString("url"), doc.getInteger("score")))
			 	  .reduceByKey((a, b) -> a + b)
			 	  .map(tuple -> new Document().append("url", tuple._1).append("score", tuple._2))
			 	  .foreachPartition(partitionRdd -> {
					Connection connection = MySQLRepositoryDAO.getConnection();
					MySQLRepositoryDAO.getInstance().insertLinkMotionResults(connection, partitionRdd);
	
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
