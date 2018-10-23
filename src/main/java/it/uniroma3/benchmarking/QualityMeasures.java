package it.uniroma3.benchmarking;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.io.Files;

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
		
		File f = new File("/home/luigi/Desktop/recall");
		try {
			Files.write("", f , StandardCharsets.UTF_8);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for(Object row: goldenData)
			if(data.contains(row))
				++relevantFound;
			else {
				try {
					Files.append(row.toString() + "\n", f, StandardCharsets.UTF_8);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		
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
}
