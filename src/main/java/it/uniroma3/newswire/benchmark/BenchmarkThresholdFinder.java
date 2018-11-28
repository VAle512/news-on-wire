package it.uniroma3.newswire.benchmark;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.newswire.benchmark.benchmarks.HyperTextualContentDinamycityPlusStability;
import it.uniroma3.newswire.benchmark.utils.QualityMeasuresCalculator;
import it.uniroma3.newswire.persistence.DAOPool;

public class BenchmarkThresholdFinder {
	@SuppressWarnings("unused")
	public static BenchmarkResult find(String dbName, String benchmarkName, JavaPairRDD<String, Double> data, double startingThreshold, QualityMeasuresCalculator measures, double neighborhoodFactor, double maxValue, int snapshot) throws InstantiationException, IllegalAccessException, IOException {
		
		double bestPrecision = 0.;
		double bestRecall = 0.;
		double bestF1Score = 0.;
		double bestThreshold = startingThreshold;
		
		for (double i = 0.0; i <= maxValue; i+=neighborhoodFactor) {
			/* first iteration */
			final double t = i;
			List<String> thresholdedData = data.filter(tuple -> {
															if (benchmarkName.equals("StabilityBenchmark"))
																return tuple._2 >= t;
															return tuple._2 <= t;
			})
					  					 	.map(x -> x._1)
					  					 	.distinct()
					  					 	.collect();
			
			double precision = measures.calculatePrecision(thresholdedData, false);
			double recall = measures.calculateRecall(thresholdedData, false);
			Double f1 = 2 * (precision * recall) / (precision + recall);
			double f1Score = (!f1.isNaN()) ? f1 : 0.;
						
			if(f1Score >= bestF1Score) {
				bestF1Score= f1Score;
				bestPrecision = precision;
				bestRecall = recall;
				bestThreshold = t;
			}
			
		}
		
		final double bT = bestThreshold;
		List<String> thresholdedData = data.filter(tuple -> {
												return tuple._2 <= bT;
													})
											.map(x -> x._1)
											.distinct()
											.collect();
		
		persist(dbName, benchmarkName, bestPrecision, bestRecall, bestF1Score, bestThreshold, snapshot);
		return new BenchmarkResult(benchmarkName, bestPrecision, bestRecall, bestF1Score, bestThreshold, snapshot);
		
	}
	
	private static void persist(String dbName, String benchmarkName, double precision, double recall, double f1, double threshold, int snapshot) {
		DAOPool.getInstance().getDAO(dbName).insertBenchmark(benchmarkName, snapshot, precision, recall, f1, threshold);		
	}
}
