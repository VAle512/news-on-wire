package it.uniroma3.newswire.classification.features;

import static org.apache.log4j.Level.INFO;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * This class is intended to compare the HyperTextual content movement of a page in relation with Stability of the same page. 
 * @author Luigi D'Onofrio
 *
 */
public class HyperTextualContentDinamycityPlusStability extends Feature{
	private static final long serialVersionUID = 510592752307143679L;

	private JavaPairRDD<String, Double> hyperTextualContentDinamycityRDD;
	private JavaPairRDD<String, Double> stabilityRDD;

	/**
	 * Constructor.
	 * @param dbName is the name of the database we are executing the Benchmark on
	 */
	public HyperTextualContentDinamycityPlusStability(String dbName) {
		super(dbName);
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.String#analyze(boolean)
	 */
	public JavaPairRDD<String, Double> calculate(boolean persistResults, int untilSnapshot) {
		log(INFO, "started");
		
		JavaPairRDD<String, Double> cached = loadCachedData();
		if(cached !=null)
			if(cached.count() != 0) {
			log(INFO, "Data in cache loaded susccessfully: " + cached.count());
			return cached.cache();
		}
		
		this.hyperTextualContentDinamycityRDD = (new PageHyperTextualReferencesDinamicity(this.database)).calculate(persistResults, untilSnapshot);
		
		this.stabilityRDD = (new Stability(this.database)).calculate(persistResults, untilSnapshot);

		/* Erase previous Combined Data */
		erasePreviousBenchmarkData(persistResults);
		
		/* This represents a puppet score in which we don't consider innerMoving score. */
		JavaPairRDD<String, Double> combined = hyperTextualContentDinamycityRDD.join(this.stabilityRDD)
					   									   .mapToPair(link -> {
					   										   double linkDinamicity = link._2._1;
					   										   double linkStability = link._2._2;
					   										   
					   										   double score = 1 / (1 + Math.exp(-linkDinamicity)) - (linkStability/2.);
					   										   score = (score < 0.) ? 0. : score;
					   										   return new Tuple2<String, Double>(link._1, score);
					   									   }).persist(StorageLevel.MEMORY_ONLY_SER());
		/* Persist result on the DB. */
		if(persistResults) {
			persist(combined);
		}
		
		log(INFO, "ended.");

		return combined;
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 */
	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score <= threshold;
	}

}
