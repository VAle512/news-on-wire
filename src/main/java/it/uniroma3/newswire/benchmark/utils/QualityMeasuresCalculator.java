package it.uniroma3.newswire.benchmark.utils;

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
public class QualityMeasuresCalculator {
	private List<String> goldenData;
	private File debugPrecision;
	private File debugRecall;
	
	/**
	 * Constructor.
	 * @param data is the RELEVANT data
	 */
	public QualityMeasuresCalculator(List<String> data) {
		this.goldenData = data;
		this.debugPrecision = new File(System.getenv("DEBUG_FOLDER") + File.separator + "precision");
		this.debugRecall = new File(System.getenv("DEBUG_FOLDER") + File.separator + "recall");
	}
	
	/**
	 * @param data is the data we want to calculate the Recall of.
	 * @return the Recall value.
	 * @throws IOException 
	 */
	public Double calculateRecall(List<String> data, boolean debug){
		long relevantFound = 0;		if(debug) {
			try {
				Files.write("", this.debugRecall, StandardCharsets.UTF_8);
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		

		for(Object row: goldenData)
			if(data.contains(row))
				++relevantFound;
			else {
				if(debug) {
					try {
						Files.append(row.toString()+"\n",this.debugRecall, StandardCharsets.UTF_8);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		
		return (double)relevantFound/(double)goldenData.size();
	}
	
	/**
	 * @param data is the data we want to calculate the Precision of.
	 * @return the Precision value.
	 * @throws IOException 
	 */
	public Double calculatePrecision(List<String> data, boolean debug){
		if(debug) {
			try {
				Files.write("", this.debugPrecision, StandardCharsets.UTF_8);
			} catch(IOException e) {
				e.printStackTrace();
			}
		}

		if(data.size()==0)
			return 0.;
		
		long relevantFound = 0;
		
		for(String row: data)
			if(goldenData.contains(row))
				++relevantFound;
			else {
				if(debug) {
					try {
						Files.append(row.toString()+"\n",this.debugPrecision, StandardCharsets.UTF_8);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		return (double)relevantFound/(double)data.size();
	}
	
	/**
	 * @param data is the data we want to calculate the F1 of.
	 * @return the F1 score.
	 * @throws IOException 
	 */
	public Double calculateF1(List<String> data, boolean debug) {
		double precision = calculatePrecision(data, debug);
		double recall = calculateRecall(data, debug);
		
		return 2 * (precision * recall) / (precision + recall);
	}
}
