package it.uniroma3.newswire.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.newswire.benchmark.utils.MetricsCalculator;
import it.uniroma3.newswire.classification.features.Feature;
import it.uniroma3.newswire.persistence.DAOPool;

public class BenchmarkThresholdFinder {
	@SuppressWarnings("unused")
	public static BenchmarkResult find(Feature benchmark, JavaPairRDD<String, Double> data, double startingThreshold, MetricsCalculator measures, double neighborhoodFactor, double maxValue, int snapshot) throws InstantiationException, IllegalAccessException, IOException {
		ForkJoinPool fjPool = new ForkJoinPool(32);
		List<Double> frontier = new ArrayList<>();
		
		/* Populate the map. */
		for (double i = 0.0; i <= maxValue; i+=neighborhoodFactor)
			frontier.add(i);
		
		String benchmarkName = benchmark.getCanonicalBenchmarkName();
		String databaseName = benchmark.getAssociatedDAO().getDatabaseName();
		
		BenchmarkCalculation task = new BenchmarkCalculation(benchmark, frontier, data, snapshot, measures);
		BenchmarkResult result = fjPool.invoke(task);
		
		persist(databaseName, benchmarkName, result.getPrecision(), result.getRecall(), result.getF1(), result.getThreshold(), snapshot);
		return result;
		
	}
	
	//TODO: Refactor usando BenchmarkResult
	private static void persist(String dbName, String benchmarkName, double precision, double recall, double f1, double threshold, int snapshot) {
		DAOPool.getInstance().getDAO(dbName).insertBenchmark(benchmarkName, snapshot, precision, recall, f1, threshold);		
	}
}
