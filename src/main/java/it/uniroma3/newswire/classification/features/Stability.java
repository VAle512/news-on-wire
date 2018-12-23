package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static org.apache.log4j.Level.INFO;

import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import scala.Tuple2;

/**
 * This class is intended for calculating the results of the Stability Analysis.
 * This analysis is based on the simple principle that the more a URLs is crawled (into snapshots) the more it is non-transient so it lasts for a long time. 
 * @author Luigi D'Onofrio
 *
 */
public class Stability extends Feature {
	private static final long serialVersionUID = -4512543190277681487L;

	/**
	 * Constructor.
	 */
	public Stability(String dbName) {
		super(dbName);
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	public JavaPairRDD<String, Double> calculate(boolean persistResults, int untilSnapshot) {
		log(INFO, "started");
		/* Erase previous Stability Data */
		if(persistResults)
			erasePreviousBenchmarkData(persistResults);

		JavaRDD<Document> rdd = loadData();
		
		if(untilSnapshot > 0)
			rdd = rdd.filter(x -> x.getInteger(snapshot.name()) <= untilSnapshot).cache();
		
		/* Retrieve the current snapshot counter */
		//int latestSnapshot = DAOPool.getInstance().getDAO(this.database).getCurrentSequence();
		
		/* Let's map into (URL, snapshot) */
		JavaPairRDD<String, Double> result = rdd.map(doc -> new Tuple2<String, Integer>(doc.getString(link.name()), doc.getInteger(snapshot.name())))
											    .distinct()
											    
											    /* Group by URL so we have (URL, (s1, s2, s3...)) 
											     * (Actually there's more but that's the basic idea.  
											     */
											    .groupBy(tuple -> tuple._1())
											  
											    /* divide the number of snapshots a URL is crawled in by the total number of snapshots taken. */
											    .mapToPair(group -> new Tuple2<>(group._1 , 
													  							(double) Iterables.asList(group._2).size() / (double) untilSnapshot));
		
		
	   /* Persisting results into the DB. */
	   if(persistResults) {
		   persist(result);
	   }
	   
	   log(INFO, "ended.");

	   
	   return result;
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 */
	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score >= threshold;
	}
	
}
