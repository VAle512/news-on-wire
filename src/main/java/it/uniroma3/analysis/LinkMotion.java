package it.uniroma3.analysis;

import static it.uniroma3.persistence.MySQLRepositoryDAO.LINKS_TABLE_NAME;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import com.google.common.collect.Sets;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import scala.Tuple2;
import scala.Tuple3;

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
								  	   							 .append("collection", row.getString(5))
								  	   							 .append("snapshot", row.getInt(6))
								  	   							 .append("date", row.getTimestamp(7)));
		
		/* Retrieve the current snapshot counter */
		int latestSnapshot = MySQLRepositoryDAO.getInstance().getCurrentSequence();
		
		/* 
		 * Here we group by (link, referring page) to isolate link occurrences 
		 * of the same page but picked in different snapshots.
		 */
		JavaRDD<Document> collectionMovement = rdd.mapToPair(row -> new Tuple2<>(new Tuple3<>(row.get("link").toString(), 
																				  row.get("referringPage").toString(), 
																				 (row.get("collection")==null || row.getString("collection").equals("")) ? null : row.get("collection").toString()),
																	 new Tuple2<>((row.get("xpath")==null) ? null : row.get("xpath").toString(),
																				  Integer.parseInt(row.get("snapshot").toString()))))
									  /* Some XPath could be null, better to remove them. */
									  .filter(row -> row._1._3()!=null && row._2._1()!=null)
									  .groupBy(pair -> pair._1)   
									  .map(group -> { 
										  /* Let's count how many "XPath's changes" there are for a link across snaphshot. */
										  Set<Tuple2<String, Integer>> xpath2snapshot = Iterables.asList(group._2).stream().map(x -> x._2).collect(Collectors.toSet());			  
										  Set<List<Tuple2<String, Integer>>> couples = 	Sets.cartesianProduct(xpath2snapshot, xpath2snapshot);	
										  
										  List<Tuple2<Integer, Integer>> erroneus = couples.stream().map(x -> new Tuple2<>(x.get(0), x.get(1)))
										  				             .filter(tuple -> tuple._1._2 < tuple._2._2)
										                             .filter(tuple -> tuple._1._1.equals(tuple._2._1))
										                             .map(tuple -> new Tuple2<>(tuple._1._2, tuple._2._2))
										                             .collect(Collectors.toList());
										  
										  int count = couples
										      .stream()
										      .map(x -> new Tuple2<>(x.get(0), x.get(1)))
										      .filter(tuple -> (tuple._1._2 < tuple._2._2)&&(!erroneus.contains(new Tuple2<>(tuple._1._2, tuple._2._2))))
										      .mapToInt(x -> {
										    	  int val = (!x._1._1.equals(x._2._1)) ? 1 : 0;
										    	  return val;
										      })
										      .sum();
	
										  return new Tuple2<>(group._1, count);
									   })
									  .groupBy(x -> x._1._1())
									  .map(x -> new Tuple2<>(x._1,Iterables.asList(x._2).stream().mapToInt(y -> y._2).sum()))
		   .map(tuple -> new Document().append("url", tuple._1).append("score", tuple._2.intValue()).append("referringPage", "nil"));
		collectionMovement.filter(x -> x.getInteger("score") != 0).take(30).forEach(System.out::println);
		/* Persist results on the DB. 
		 * WARNING: referringPage not persisted but could be useful! 
		 */
		if(persist) {
			collectionMovement.mapToPair(doc -> new Tuple2<>(doc.getString("url"), doc.getInteger("score")))
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
		
		return collectionMovement;
	}
}
