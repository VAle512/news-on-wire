package it.uniroma3.newswire.benchmark.utils;

import java.util.List;

/**
 * This class does quality measures calculations such as Precision and Recall.
 * @author Luigi D'Onofrio
 *
 */
public class QualityMeasuresCalculator {
	private List<String> goldenData;
	
	/**
	 * Constructor.
	 * @param data is the RELEVANT data
	 */
	public QualityMeasuresCalculator(List<String> data) {
		this.goldenData = data;
	}
	
	/**
	 * @param data is the data we want to calculate the Recall of.
	 * @return the Recall value.
	 */
	public Double calculateRecall(List<String> data) {
		long relevantFound = 0;
		
		for(Object row: goldenData)
			if(data.contains(row))
				++relevantFound;
		
		return (double)relevantFound/(double)goldenData.size();
	}
	
	/**
	 * @param data is the data we want to calculate the Precision of.
	 * @return the Precision value.
	 */
	public Double calculatePrecision(List<String> data) {
		long relevantFound = 0;
		
		for(Object row: goldenData)
			if(data.contains(row))
				++relevantFound;

		return (double)relevantFound/(double)data.size();
	}
	
	/**
	 * @param data is the data we want to calculate the F1 of.
	 * @return the F1 score.
	 */
	public Double calculateF1(List<String> data) {
		double precision = calculatePrecision(data);
		double recall = calculateRecall(data);
		
		return 2 * (precision * recall) / (precision + recall);
	}
}
