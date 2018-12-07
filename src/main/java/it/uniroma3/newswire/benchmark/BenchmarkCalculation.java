package it.uniroma3.newswire.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.newswire.benchmark.utils.QualityMeasuresCalculator;

public class BenchmarkCalculation extends RecursiveTask<BenchmarkResult> {
	private static final long serialVersionUID = -5575576964001060975L;
	private static final int FRONTIER_THRESHOLD = 128;

	private static Logger logger = Logger.getLogger(BenchmarkCalculation.class);

	private String benchmarkName;
	private int snapshot;
	private JavaPairRDD<String, Double> data;
	private QualityMeasuresCalculator measuresCalculator;
	private List<Double> frontier;


	public BenchmarkCalculation(String benchmarkName, List<Double> frontier, JavaPairRDD<String, Double> currentData, int snapshot, QualityMeasuresCalculator measuresCalculator) {
		this.benchmarkName = benchmarkName;
		this.snapshot = snapshot;
		this.data = currentData;
		this.measuresCalculator = measuresCalculator;

		this.frontier = frontier;
	}


	@Override
	protected BenchmarkResult compute() {
		if(frontier.size() <= FRONTIER_THRESHOLD) {
			return this.getBestScore();
		} else {
			return invokeAll(this.getSubTasks())
					   .stream()
					   .map(BenchmarkCalculation::join)
					   .sorted((bench1, bench2) -> bench1.getF1().compareTo(bench2.getF1()))
					   .findFirst()
					   .get();
		}
	}

	private BenchmarkResult getBestScore() {
		BenchmarkResult bestBenchmark = null;
		/* Execute all benchmarks */
		for(Double t: this.frontier) {

			/* Get the thresholded data to the current threshold t. */
			List<String> thresholdedData = data.filter(tuple -> {
														if (benchmarkName.equals("StabilityBenchmark"))
															return tuple._2 >= t;
															return tuple._2 <= t;
													})
															.map(x -> x._1)
															.distinct()
															.collect();

			double precision = measuresCalculator.calculatePrecision(thresholdedData, false);
			double recall = measuresCalculator.calculateRecall(thresholdedData, false);
			Double f1 = 2 * (precision * recall) / (precision + recall);
			double f1Score = (!f1.isNaN()) ? f1 : 0.;

			BenchmarkResult result = new BenchmarkResult(this.benchmarkName, precision, recall, f1Score, t, this.snapshot);
			if(bestBenchmark == null)
				bestBenchmark = result;
			else
				if(result.getF1() > bestBenchmark.getF1())
					bestBenchmark = result;
		}
		
		return bestBenchmark;
	}
	
	private List<BenchmarkCalculation> getSubTasks() {
		int mid = frontier.size() / 2;

		List<Double> leftFrontier = frontier.subList(0, mid);
		List<Double> rightFrontier = frontier.subList(mid + 1, frontier.size() - 1);

		BenchmarkCalculation left = new BenchmarkCalculation(this.benchmarkName, leftFrontier, this.data, snapshot, this.measuresCalculator);
		BenchmarkCalculation right = new BenchmarkCalculation(this.benchmarkName, rightFrontier, this.data, snapshot, this.measuresCalculator);

		List<BenchmarkCalculation> subTasks = new ArrayList<>();
		subTasks.add(left);
		subTasks.add(right);
		
		return subTasks;
	}

}
