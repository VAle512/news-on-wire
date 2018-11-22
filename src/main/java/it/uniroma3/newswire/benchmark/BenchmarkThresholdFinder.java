package it.uniroma3.newswire.benchmark;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.newswire.benchmark.utils.QualityMeasuresCalculator;
import it.uniroma3.newswire.persistence.DAOPool;

public class BenchmarkThresholdFinder {
	@SuppressWarnings("unused")
	public static void find(String dbName, String benchmarkName, JavaPairRDD<String, Double> data, double startingThreshold, QualityMeasuresCalculator measures, double neighborhoodFactor, double maxValue, int snapshot) throws InstantiationException, IllegalAccessException, IOException {
		double currentThreshold = startingThreshold;
		
		/* first iteration */
		List<String> thresholdedData = data.filter(tuple -> tuple._2 <= startingThreshold)
				  					 	.map(x -> x._1)
				  					 	.distinct()
				  					 	.collect();
		
		double precision = measures.calculatePrecision(thresholdedData);
		double recall = measures.calculateRecall(thresholdedData);
		double f1 = 2*(precision * recall)/(precision + recall);
		
		/* end of first iteration. */
		
		/* settings */
		int j = 200;
		double Temperature = j;
		
		double bestF1SCore = f1;
		double bestPrecision = precision;
		double bestRecall = recall;
		
		double cycleBestF1 = 0.;
		double cycleBestPrecision = 0.;
		double cycleBestRecall = 0.;
		
		double bestThreshold = startingThreshold;
		
		if(neighborhoodFactor==0.) {
			persist(dbName, benchmarkName, bestPrecision, bestRecall, bestF1SCore, bestThreshold, snapshot);
			return;
		}
		
		do {
			do {
				/* 
				 * Select a neighbor of the current point. 
				 * It has to be greater than zero and lower or equal to the maximum value of the Benchmark we are executing.
				 */
				double tempThreshold = 0.;
				do {
					Double step = ThreadLocalRandom.current().nextDouble(-neighborhoodFactor, neighborhoodFactor);
					tempThreshold = currentThreshold + step;
				}while(tempThreshold < 0 || tempThreshold > maxValue);

				final double t = tempThreshold;
				thresholdedData = data.filter(tuple -> tuple._2 <= t)
								   	  .map(x -> x._1)
								   	  .distinct()
								   	  .collect();
				
				precision = measures.calculatePrecision(thresholdedData);
				recall = measures.calculateRecall(thresholdedData);
				f1 = measures.calculateF1(thresholdedData);
				
				/* just a shitty protection from NaNs */
				if((new Double(f1)).isNaN())
					f1=0.;
				
				/* This is a variant for the Simulated Annealing Algorithm:
				 * we constantly check for maximum registered value for f1.
				 * This way we don't let our "temporary" best to get too dirty.
				 */
				if(f1 > bestF1SCore) {
					bestF1SCore = f1;
					bestPrecision = precision;
					bestRecall = recall;
					bestThreshold = t;
				}
				
				/* This is the main algorithm check */
				if(cycleBestF1 < f1) {
					cycleBestF1 = f1;
					cycleBestPrecision = precision;
					cycleBestRecall = recall;
					currentThreshold = t;
					
				} else if (Random.class.newInstance().nextDouble() < Math.exp((f1 - cycleBestF1)*1000/j)) {
					cycleBestF1 = f1;
					cycleBestPrecision = precision;
					cycleBestRecall = recall;
					currentThreshold = t;
				}
			/* Repeat this until we have an improvement of 0.2 */
			} while((f1 - cycleBestF1) > 0.02);
				
				Temperature = Temperature - 0.7;
				--j;
					
			} while(j>0);
		
		persist(dbName, benchmarkName, bestPrecision, bestRecall, bestF1SCore, bestThreshold, snapshot);
		
	}
	
	private static void persist(String dbName, String benchmarkName, double precision, double recall, double f1, double threshold, int snapshot) {
		DAOPool.getInstance().getDAO(dbName).insertBenchmark(benchmarkName, snapshot, precision, recall, f1, threshold);		
	}
}
