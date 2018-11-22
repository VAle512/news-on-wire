package it.uniroma3.newswire.benchmark.benchmarks;

import org.apache.spark.api.java.JavaPairRDD;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import scala.Tuple2;

/**
 * This class is intended to compare the HyperTextual content movement of a page in relation with Stability of the same page. 
 * @author Luigi D'Onofrio
 *
 */
public class HyperTextualContentDinamycityPlusStability extends Benchmark{
	private static final long serialVersionUID = 510592752307143679L;

	/**
	 * Constructor.
	 * @param dbName is the name of the database we are executing the benchmark on
	 */
	public HyperTextualContentDinamycityPlusStability(String dbName) {
		this.database = dbName;
		this.resultsTable = this.getClass().getSimpleName();
		init();
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Benchmark#analyze(boolean)
	 */
	public JavaPairRDD<String, Double> analyze(boolean persist, int untilSnapshot) {
		
		JavaPairRDD<String, Double> hyperTextualContentDinamycityRDD = (new HyperTextualContentDinamicity(this.database)).analyze(persist, untilSnapshot);
		JavaPairRDD<String, Double> stabilityRDD = (new Stability(this.database)).analyze(persist, untilSnapshot);

		/* Erase previous Combined Data */
		if(persist) {
			DAO dao = DAOPool.getInstance().getDAO(this.database);
			if(dao.checkTableExists(this.resultsTable))
				dao.cleanTable(this.resultsTable);
			else
				dao.createAnalysisTable(this.resultsTable);
		}
		
		
		/* This rdd represents all the "movements" an hypertextual link does in the entire environment. */
		JavaPairRDD<String, Integer> selfMovement = hyperTextualContentDinamycityRDD.groupBy(triple -> triple._1())
														  							.mapToPair(group -> new Tuple2<>(group._1, Iterables.asList(group._2).stream().mapToInt(x -> x._2().intValue()).sum()));

		/* 
		 * This rdd combines the three informations:
		 * - staticity:x
		 * - innerMoving:y
		 * - selfMoving:z
		 * 				  1            x
		 * score = _______________ _ _____
		 * 		   (1 + e^(-(z/y))	   2
		 */
		
		/* This represents a puppet score in which we don't consider innerMoving score. */
		JavaPairRDD<String, Double> combined = selfMovement.join(stabilityRDD)
					   									   .mapToPair(x -> new Tuple2<String, Double>(x._1, (1 /(1 + Math.exp(-(x._2._1) + 0)) - x._2._2/2)));
		/* Persist result on the DB. */
		if(persist) {
			persist(combined, this.resultsTable);
		}
		
		return combined;
	}

	public String getCanonicalBenchmarkName() {
		return HyperTextualContentDinamycityPlusStability.class.getSimpleName()+"Benchmark";
	}
	

	public String getBenchmarkSimpleName() {
		return HyperTextualContentDinamycityPlusStability.class.getSimpleName();
	}

}
