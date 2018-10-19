package it.uniroma3.analysis;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import scala.Tuple2;
import scala.Tuple3;

/**
 * This class is intended to compare the content movement of a page in relation with movement of the same page, in the pages which point to it.
 * @author Luigi D'Onofrio
 *
 */
public class CombinedAnalysis {
	public static JavaPairRDD<String, Double> analyze() {
		JavaRDD<Document> IPSAnalysisResult = IntraPageStaticityAnalysis.analyze();
		
		/* Map values to more understandable and usable form. */
		JavaRDD<Tuple3<String, String, Integer>> rdd2 = IPSAnalysisResult.map(doc -> new Tuple3<>(doc.get("url").toString(), doc.getString("referringPage"), doc.getInteger("ips")));
		JavaPairRDD<String, Integer> selfMovement = rdd2.groupBy(triple -> triple._1())
															.mapToPair(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x -> x._3()).sum()));
		/* Let's study */
		JavaPairRDD<String, Integer> innerMovement = rdd2.groupBy(triple -> triple._2().toString()) /* Let's group by referring page */
														 .mapToPair(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x -> x._3()).sum()));
		
		return innerMovement.join(selfMovement) /*  */
					 .mapToPair(triple -> new Tuple2<>(triple._1, (double) triple._2._2 / (double)triple._2._1));	
		}
}
