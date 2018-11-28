package it.uniroma3.newswire.benchmark;

public class BenchmarkResult {
	private String benchmark;
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
	public BenchmarkResult(String benchmarkName, Double precision, Double recall, Double f1, Double thresholsd, int snapshot) {
		super();
		this.benchmark = benchmarkName;
		this.precision = precision;
		this.recall = recall;
		this.f1 = f1;
		this.threshold = thresholsd;
		this.snapshot = snapshot;
	}

	public String getBenchmark() {
		return benchmark;
	}

	public void setBenchmark(String benchmark) {
		this.benchmark = benchmark;
	}

	public Double getPrecision() {
		return precision;
	}

	public void setPrecision(Double precision) {
		this.precision = precision;
	}

	public Double getRecall() {
		return recall;
	}

	public void setRecall(Double recall) {
		this.recall = recall;
	}

	public Double getF1() {
		return f1;
	}

	public void setF1(Double f1) {
		this.f1 = f1;
	}

	public Double getThreshold() {
		return threshold;
	}

	public void setThreshold(Double threshold) {
		this.threshold = threshold;
	}

	public int getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(int snapshot) {
		this.snapshot = snapshot;
	}

}
