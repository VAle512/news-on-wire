package it.uniroma3.analysis;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import scala.Tuple2;
import scala.Tuple3;

/**
 * This class is intended to compare the content movement of a page in relation with movement of the same page, 
 * in the pages which point to it.
 * @author Luigi D'Onofrio
 *
 */
public class Combined extends Analysis{
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	public JavaRDD<Document> analyze(boolean persist) {
		JavaRDD<Document> IPSAnalysisResult = (new LinkMotion()).analyze(persist);
		JavaRDD<Document> SimpleStaticityAnalysisResult = (new LinkAbsoluteTransiency()).analyze(persist);
		
		/* Erase previous data */
		if(persist)
			MySQLRepositoryDAO.getInstance().createCombinedTable();
		
		/* Map values to more understandable and usable form. */
		JavaPairRDD<String, Double> stabilities = SimpleStaticityAnalysisResult.mapToPair(doc -> new Tuple2<>(doc.getString("url"), doc.getDouble("score")));
		JavaRDD<Tuple3<String, String, Integer>> ipsRDD = IPSAnalysisResult.map(doc -> new Tuple3<>(doc.get("url").toString(), doc.getString("referringPage"), doc.getInteger("score")));
		
		/* This rdd represents all the "movements" a link does in the entire environment. */
		JavaPairRDD<String, Integer> selfMovement = ipsRDD.groupBy(triple -> triple._1())
														  .mapToPair(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x -> x._3()).sum()));
		
		/* This rdd represents all the "movements" a link contains in his content. */
//		JavaPairRDD<String, Integer> innerMovement = rdd2.groupBy(triple -> triple._2().toString()) /* Let's group by referring page */
//														 .mapToPair(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x -> x._3()).sum()));
		/* 
		 * This rdd combines the three informations:
		 * - staticity:x
		 * - innerMoving:y
		 * - selfMoving:z
		 * 				  1            x
		 * score = _______________ _ _____
		 * 		   (1 + e^(-(z/y))	   2
		 */
//		JavaPairRDD<String, Double> combined = innerMovement.join(selfMovement) /*  */
//					 										.mapToPair(triple -> new Tuple2<>(triple._1, (double) triple._2._2 / (double)((triple._2._1==0.) ? 0.1 : triple._2._1)));
		
		/* This represents a puppet score in which we don't consider innerMoving score. */
		JavaPairRDD<String, Double> combined = selfMovement.join(stabilities)
					   									   .mapToPair(x -> new Tuple2<String, Double>(x._1, (1 /(1 + Math.exp(-x._2._1)) - x._2._2/2)));
		/* Persist result on the DB. */
		if(persist) {
			combined.map(tuple -> new Document().append("url", tuple._1).append("score", tuple._2))
					.foreachPartition(partitionRdd ->{
						Connection connection = MySQLRepositoryDAO.getConnection();
						MySQLRepositoryDAO.getInstance().insertCombinedResults(connection, partitionRdd);
	
						try {
							connection.close();
						} catch (SQLException e) {
							logger.error(e.getMessage());
						}
					});
		}
		
		return combined.map(tuple -> new Document().append("url", tuple._1).append("score", tuple._2));
	}
}
