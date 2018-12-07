package it.uniroma3.newswire.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.newswire.benchmark.utils.QualityMeasuresCalculator;
import it.uniroma3.newswire.persistence.DAOPool;

public class BenchmarkThresholdFinder {
	@SuppressWarnings("unused")
	public static BenchmarkResult find(String dbName, String benchmarkName, JavaPairRDD<String, Double> data, double startingThreshold, QualityMeasuresCalculator measures, double neighborhoodFactor, double maxValue, int snapshot) throws InstantiationException, IllegalAccessException, IOException {
		ForkJoinPool fjPool = new ForkJoinPool(32);
		List<Double> frontier = new ArrayList<>();
		
		/* Populate the map. */
		for (double i = 0.0; i <= maxValue; i+=neighborhoodFactor)
			frontier.add(i);
		
		BenchmarkCalculation task = new BenchmarkCalculation(benchmarkName, frontier, data, snapshot, measures);
		BenchmarkResult result = fjPool.invoke(task);
		
		persist(dbName, benchmarkName, result.getPrecision(), result.getRecall(), result.getF1(), result.getThreshold(), snapshot);
		return result;
		
	}
	
	//TODO: Refactor usando BenchmarkResult
	private static void persist(String dbName, String benchmarkName, double precision, double recall, double f1, double threshold, int snapshot) {
		DAOPool.getInstance().getDAO(dbName).insertBenchmark(benchmarkName, snapshot, precision, recall, f1, threshold);		
	}
}
