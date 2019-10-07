package it.uniroma3.newswire.benchmark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.newswire.benchmark.utils.MetricsCalculator;
import it.uniroma3.newswire.classification.features.Feature;

public class BenchmarkCalculation extends RecursiveTask<BenchmarkResult> implements Serializable{
	private static final long serialVersionUID = -5575576964001060975L;
	private static final int FRONTIER_THRESHOLD = 128;

	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(BenchmarkCalculation.class);

	private Feature benchmark;
	private int snapshot;
	private JavaPairRDD<String, Double> data;
	private MetricsCalculator measuresCalculator;
	private List<Double> frontier;


	/**
	 * Constructor.
	 * @param benchmark - A reference to the particular benchmark we are calculating best value of.
	 * @param frontier - Is the ensemble of thresholds we need to calculate the value of.
	 * @param currentData - Are the scores //TODO: It sucks a bit. Check responsabilities.
	 * @param snapshot - Is the current snapshot
	 * @param measuresCalculator - Is the calculator for Precision, Recall and F1-Score. //TODO: Also this sucks.
	 */
	public BenchmarkCalculation(Feature benchmark, List<Double> frontier, JavaPairRDD<String, Double> currentData, int snapshot, MetricsCalculator measuresCalculator) {
		this.benchmark = benchmark;
		this.snapshot = snapshot;
		this.data = currentData;
		this.measuresCalculator = measuresCalculator;

		this.frontier = frontier;
	}


	/* (non-Javadoc)
	 * @see java.util.concurrent.RecursiveTask#compute()
	 */
	@Override
	protected BenchmarkResult compute() {
		/* If the frontier has a reasonable size to get processed sequentially */
		if(frontier.size() <= FRONTIER_THRESHOLD) {
			return this.getBestScore();
		} else {
		/* Else split in two and execute */	
			return invokeAll(this.split())
					.stream()
					.map(BenchmarkCalculation::join)
					/* We order the thresholds by the F1-Score and we get the first one, the best one. */
					.sorted((bench1, bench2) -> (-1) * bench1.getF1().compareTo(bench2.getF1()))
					.findFirst()
					.get();
		}
	}

	/**
	 * This returns the highest F1-Score of the calculated pack. 
	 * @return the corresponding {@link BenchmarkResult}.
	 */
	private BenchmarkResult getBestScore() {
		BenchmarkResult bestBenchmark = null;
		/* Execute all benchmarks */
		for(Double t: this.frontier) {

			/* Get the thresholded data to the current threshold t. */
			/* FIXME: Capire se funziona il predicato personalizzato */
			List<String> thresholdedData = data.filter(tuple -> this.benchmark.isThresholded(tuple._2, t))
											   .map(x -> x._1)
											   .distinct()
											   .collect();
			
			/* Get the scorings */
			double precision = measuresCalculator.precision(thresholdedData);
			double recall = measuresCalculator.recall(thresholdedData);
			Double f1 = 2 * (precision * recall) / (precision + recall);
			double f1Score = (!f1.isNaN()) ? f1 : 0.;
			
			BenchmarkResult result = new BenchmarkResult(this.benchmark, precision, recall, f1Score, t, this.snapshot);

			if(bestBenchmark == null)
				bestBenchmark = result;
			else
				if(result.getF1() > bestBenchmark.getF1()) {
					System.out.println("new best: " + result.getF1());
					bestBenchmark = result;
				}
		}

		return bestBenchmark;
	}

	/**
	 * Split the task in two subtasks.
	 * @return a {@link List<BenchmarkCalculation>} of exactly two tasks.
	 */
	private List<BenchmarkCalculation> split() {
		/* let's find teh centre of our list of thresholds */
		int mid = frontier.size() / 2;
		
		/* Divide into two sub-lists */
		List<Double> leftFrontier = new ArrayList<>(frontier.subList(0, mid));
		List<Double> rightFrontier = new ArrayList<>(frontier.subList(mid + 1, frontier.size() - 1));
		
		/* Assign those lists to two new sub-takss */
		BenchmarkCalculation left = new BenchmarkCalculation(this.benchmark, leftFrontier, this.data, snapshot, this.measuresCalculator);
		BenchmarkCalculation right = new BenchmarkCalculation(this.benchmark, rightFrontier, this.data, snapshot, this.measuresCalculator);
		
		/* Add those two sub-tasks to a list of two elements */
		List<BenchmarkCalculation> subTasks = new ArrayList<>(2);
		subTasks.add(left);
		subTasks.add(right);

		return subTasks;
	}

}
