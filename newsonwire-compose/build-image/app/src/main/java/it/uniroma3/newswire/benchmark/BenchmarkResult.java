package it.uniroma3.newswire.benchmark;

import it.uniroma3.newswire.classification.features.Feature;

/**
 * This class models a benchmark result. 
 * It is composed of a precision value, recall, f1 and the best threshold for the benchmark itself.
 * It is also provided the name of the benchmark to identify the kind of this element as long as the snapshot of interest.
 * @author root
 *
 */
public class BenchmarkResult {
	private String benchmarkName;
	private Feature benchmark;
	private Double precision;
	private Double recall;
	private Double f1;
	private Double threshold;
	private int snapshot;
	
	/**
	 * Constructor.
	 * @param benchmarkName
	 * @param precision
	 * @param recall
	 * @param f1
	 * @param thresholsd
	 */
	public BenchmarkResult(Feature benchmark, Double precision, Double recall, Double f1, Double thresholsd, int snapshot) {
		super();
		this.benchmark = benchmark;
		this.benchmarkName = benchmark.getCanonicalBenchmarkName();
		this.precision = precision;
		this.recall = recall;
		this.f1 = f1;
		this.threshold = thresholsd;
		this.snapshot = snapshot;
	}

	public String getBenchmarkName() {
		return benchmarkName;
	}
	
	public Feature getBenchmark() {
		return this.benchmark;
	}

	public Double getPrecision() {
		return precision;
	}

	public Double getRecall() {
		return recall;
	}

	public Double getF1() {
		return f1;
	}

	public Double getThreshold() {
		return threshold;
	}


	public int getSnapshot() {
		return snapshot;
	}
	
	public String toString() {
		return "p: " + this.precision + " --- r: " + this.recall + " --- f1: " + this.getF1() + " --- t: " + this.threshold;
	}

}
