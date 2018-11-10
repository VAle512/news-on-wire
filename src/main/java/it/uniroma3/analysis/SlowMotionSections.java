package it.uniroma3.analysis;

import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import scala.Tuple2;

public class SlowMotionSections extends Analysis{
	
	public SlowMotionSections() {
		init();
	}
	
	@Override
	public JavaRDD<Document> analyze(boolean persist) {
//		JavaRDD<Document> rdd =  loadData(LINKS_TABLE_NAME)
//			  	   /* Let's map the Row in a Document made of the following fields.
//			  	    * It's just easier. Nothing more.
//			  	    */
//			  	   .map(row -> new Document().append("id", row.getInt(0))
//					                    	 .append("link", row.getString(1))
//					                    	 .append("referringPage", row.getString(2))
//					                    	 .append("relativeLink", row.getString(3))
//			  	   							 .append("xpath", row.getString(4))
//			  	   							 .append("snapshot", row.getInt(5))
//			  	   							 .append("date", row.getTimestamp(6)));
//		JavaRDD<String> uniqueLinks = rdd.map(doc -> new Tuple2<>(doc.getString("link"), doc.getString("referringPage")))
//										 .groupBy(tuple -> tuple._1)
//										 .filter(group2couples -> Iterables.asList(group2couples._2).stream().map(x -> x._2).distinct().count() == 1)
//										 .map(x -> x._1);		
		JavaPairRDD<String, Integer> scores = loadData("IntraPageStaticity")
								               .mapToPair(row -> new Tuple2<>(row.getString(0), row.getInt(1)));
		
		JavaPairRDD<String, String> referrings = loadData(MySQLRepositoryDAO.LINKS_TABLE_NAME)
												 .mapToPair(row -> new Tuple2<>(row.getString(1), row.getString(2)))
												 .distinct();

		scores.join(referrings)
			  .groupBy(x -> x._2._2)
			  .mapToPair(x -> new Tuple2<>(x._1, Iterables.asList(x._2).stream().mapToInt(y -> y._2._1).sum()))
			  .join(scores)
			  .mapToPair(x -> new Tuple2<>(x._1, (x._2._1==0) ? Double.MAX_VALUE : (double)x._2._2/(double)x._2._1))
			  .filter(x -> x._2>0.)
			  .take(200)
			  .forEach(System.out::println);
		
		return null;
	}

}
