package it.uniroma3.analysis;

import static it.uniroma3.persistence.MySQLRepositoryDAO.LINKS_TABLE_NAME;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import scala.Tuple2;
import scala.Tuple3;

public class InterPageMotion extends Analysis implements Serializable {	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9094868662523555115L;

	/**
	 * Constructor.
	 */
	public InterPageMotion() {
		init();
	}
	
	@Override
	public JavaRDD<Document> analyze(boolean persist) {
		/* Erase previous Link Motion Data */
		
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
		
		JavaPairRDD<String, List<Tuple2<String, Tuple2<Integer, Integer>>>> result = rdd.map(doc -> new Tuple3<>(doc.getString("link"), 
				                                            													 doc.getString("referringPage"), 
				                                            													 doc.getInteger("snapshot")))
									  .groupBy(tuple3 -> tuple3._1())
									  .mapToPair(group -> {
										  List<Tuple2<String, Tuple2<Integer, Integer>>> min2max = new ArrayList<>();
 			  
										  min2max = Iterables.asList(group._2)
												  	         .stream()
												  	         .collect(Collectors.groupingBy(Tuple3<String, String, Integer>::_2))
												  	         .entrySet()
												  	         .stream()
												  	         .map(entry ->{
												  	        	 int min = minSnapshot(Iterables.asList(entry.getValue()));
												  	        	 int max = maxSnapshot(Iterables.asList(entry.getValue()));
												  	        	 return new Tuple2<>(entry.getKey(), new Tuple2<>(min, max));
												  	         }).collect(Collectors.toList());
//										  min2max.sort((a, b) -> {
//											  int maxA = a._2._2;
//											  int minB = b._2._1;
//											  if(minB > maxA)
//												  return -1;
//											  if(minB < maxA)
//												  return 1;
//										  }
										  return new Tuple2<String, List<Tuple2<String, Tuple2<Integer, Integer>>>>(group._1, min2max);
									  });
		JavaPairRDD<String, Integer> changes = result.mapToPair(x -> {
													int count = 0;
													for(int i=0; i< x._2.size()-1; ++i)
														if (!x._2.get(i)._1.equals(x._2.get(i+1)))
															if(x._2.get(i)._2._2 <= x._2.get(i+1)._2._1)
																++count;
													return new Tuple2<String, Integer>(x._1, count);
												});
		
		
		JavaPairRDD<String, Integer> totals = changes.mapToPair(x -> new Tuple2<>(x._1, x._2));

		return totals.map(x -> new Document().append("link", x._1).append("score", x._2));
	}
	
	private int minSnapshot(List<Tuple3<String, String, Integer>> l) {
		int min = Integer.MAX_VALUE;
		for(Tuple3<String, String, Integer> tuple: l) {
			if (tuple._3() < min)
				min = tuple._3();
		}
		
		return min;
	}
	
	private int maxSnapshot(List<Tuple3<String, String, Integer>> l) {
		int max = 0;
		for(Tuple3<String, String, Integer> tuple: l) {
			if (tuple._3() > max)
				max = tuple._3();
		}
		
		return max;
	}

}
