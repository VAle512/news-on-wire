package it.uniroma3.newswire.benchmark.benchmarks;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

/**
 * This class is intended to compare the HyperTextual content movement of a page in relation with Stability of the same page. 
 * @author Luigi D'Onofrio
 *
 */
public class HyperTextualContentDinamycityPlusStability extends Benchmark{
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
	public JavaPairRDD<String, Double> analyze(boolean persistResults, int untilSnapshot) {
		
		this.hyperTextualContentDinamycityRDD = (new HyperTextualContentDinamicity(this.database)).analyze(persistResults, untilSnapshot);
		
		this.stabilityRDD = (new Stability(this.database)).analyze(persistResults, untilSnapshot);

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
					   									   });
		/* Persist result on the DB. */
		if(persistResults) {
			persist(combined);
		}
		
		return combined;
	}

}
