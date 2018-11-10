package it.uniroma3.analysis;

import static it.uniroma3.persistence.MySQLRepositoryDAO.LINKS_TABLE_NAME;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
	
	public Combined() {
		init();
	}
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	public JavaRDD<Document> analyze(boolean persist) {
		JavaRDD<Document> InterPage = (new InterPageMotion()).analyze(persist);
		
		JavaRDD<Document> IPSAnalysisResult = (new LinkMotion()).analyze(persist);
		JavaRDD<Document> SimpleStaticityAnalysisResult = (new LinkAbsoluteTransiency()).analyze(persist);
		
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
			  	   							 .append("date", row.getTimestamp(6)))
			  	   .cache();
		
		/* Erase previous data */
		if(persist)
			MySQLRepositoryDAO.getInstance().createCombinedTable();
		
		/* Map values to more understandable and usable form. */
		JavaPairRDD<String, Double> stabilities = SimpleStaticityAnalysisResult.mapToPair(doc -> new Tuple2<>(doc.getString("url"), doc.getDouble("score")));
		JavaRDD<Tuple3<String, String, Integer>> ipsRDD = IPSAnalysisResult.map(doc -> new Tuple3<>(doc.get("url").toString(), doc.getString("referringPage"), doc.getInteger("score")));
		
		/* This rdd represents all the "movements" a link does in the entire environment. */
		JavaPairRDD<String, Integer> selfMovement = ipsRDD.groupBy(triple -> triple._1())
														  .mapToPair(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x -> x._3()).sum()));
		JavaPairRDD<String, Integer> chg = InterPage.mapToPair(x -> new Tuple2<>(x.getString("link"), x.getInteger("score")));
//		selfMovement = selfMovement.join(pageChanges).mapToPair(x -> new Tuple2<>(x._1, x._2._1 + x._2._2));
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
		JavaPairRDD<String, Double> combined = selfMovement.join(stabilities).join(chg)
					   									   .mapToPair(x -> new Tuple2<String, Double>(x._1, (1 /(1 + Math.exp(-(x._2._1._1) + 0)) - x._2._1._2/2)));
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
