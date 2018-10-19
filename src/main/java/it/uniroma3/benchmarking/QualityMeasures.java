package it.uniroma3.benchmarking;

import java.util.List;

/**
 * This class does quality measures calculations such as Precision and Recall.
 * @author Luigi D'Onofrio
 *
 */
public class QualityMeasures {
	private static List<String> goldenData;
	
	/**
	 * Constructor.
	 * @param data is the RELEVANT data
	 */
	public QualityMeasures(List<String> data) {
		goldenData = data;
	}
	
	/**
	 * @param data is the data we want to calculate the Recall of.
	 * @return the Recall value.
	 */
	public Double calculateRecall(List<String> data) {
		long relevantFound = 0;

		for(Object row: data)
			if(goldenData.contains(row))
				++relevantFound;
		return (double)relevantFound/(double)goldenData.size();
	}
	
	/**
	 * @param data is the data we want to calculate the Precision of.
	 * @return the Precision value.
	 */
	public Double calculatePrecision(List<String> data) {
		long relevantFound = 0;
		
		for(Object row: data)
			if(goldenData.contains(row))
				++relevantFound;
		return (double)relevantFound/(double)data.size();
	}
}
