package it.uniroma3.newswire.benchmark.utils;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envDebug;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.io.Files;

import it.uniroma3.newswire.classification.features.Feature;
import scala.Tuple2;

/**
 * This class does quality measures calculations such as Precision and Recall.
 * @author Luigi D'Onofrio
 *
 */
public class MetricsCalculator implements Serializable {
	private static final long serialVersionUID = 2335331922807893863L;
	private List<String> cachedGoldenData;
	private String website;
	private Feature benchmark;
	private boolean debug;
	
	/* File in which we can write the wrong values concerning recall for debug purposes */
	private File debugRecallFile;
	
	/* File in which we can write the wrong values concerning precision for debug purposes */
	private File debugPrecisionFile;
	
	
	/*
	 * 						|		actual positives 	 |		actual negatives	 |
	 * --------------------- ---------------------------- ---------------------------
	 *	predicted positives	|		true positives (TP)	 |		false positives (FP) |				
	 *	predicted negatives	|		false negatives (FN) |		true negatives (TN)  |
	 */
	private int[][] confusionMatrix;
	
	
	
	
	/**
	 * Constructor.
	 * @param goldenData is the GOLDEN data. This way we can cache it instead of reading it every time.
	 */
	public MetricsCalculator(String website, Feature benchmark, List<String> goldenData, boolean debug) {
		this.website = website;
		this.cachedGoldenData = goldenData;
		this.debug = debug;
		this.benchmark = benchmark;
		
		/* If we want to debug, we can cerate the two custom debug files */
		if(this.debug) {
			this.debugPrecisionFile = new File(System.getenv(envDebug) + File.separator + this.website + "_precision");
			this.debugRecallFile = new File(System.getenv(envDebug) + File.separator + this.website +  "_recall");
		}
	}
	
	private void generateConfusionMatrix(List<Tuple2<String, Double>> dataPoints, double threshold) {
		int truePositives = 0;
		int falsePositives = 0;
		int trueNegatives = 0;
		int falseNegatives = 0;
		
		List<String> positives = dataPoints.stream()
										   .filter(tuple -> this.benchmark.isThresholded(tuple._2, threshold))
										   .map(tuple -> tuple._1)
										   .collect(Collectors.toList());
		
		List<String> negatives = dataPoints.stream()
				   						   .filter(tuple -> !this.benchmark.isThresholded(tuple._2, threshold))
				   						   .map(tuple -> tuple._1)
				   						   .collect(Collectors.toList());
		
		for(String url: positives)
			if(this.cachedGoldenData.contains(url))
				++truePositives;
			else
				++falsePositives;
		
		
		for(String url: negatives)
			if(!this.cachedGoldenData.contains(url))
				++trueNegatives;
			else
				++falseNegatives;
			
		
		this.confusionMatrix = new int [2][2];
		this.confusionMatrix[0][0] = truePositives;
		this.confusionMatrix[0][1] = falsePositives;
		this.confusionMatrix[1][0] = falseNegatives;
		this.confusionMatrix[1][1] = trueNegatives;
		
	}
		
	/**
	 * @param positives is the data we want to calculate the Recall of.
	 * @return the Recall value. 
	 */
	public Double recall(List<String> positives){
		long relevantFound = 0;	
		
		this.eraseDebugFile(this.debugRecallFile);
		
		for(String url: this.cachedGoldenData)
			if(positives.contains(url))
				++relevantFound;
			else {
				this.appendRecallDebug(url);
			}
		
		return (double)relevantFound/(double)cachedGoldenData.size();
	}
	
	/**
	 * @param positives is the data we want to calculate the Precision of.
	 * @return the Precision value.
	 */
	public Double precision(List<String> positives){
		this.eraseDebugFile(this.debugPrecisionFile);

		if(positives.size()==0)
			return 0.;
	
		long relevantFound = 0;
		
		for(String url: positives)
			if(cachedGoldenData.contains(url))
				++relevantFound;
			else {
				this.appendPrecisionDebug(url);
			}

		return (double)relevantFound/(double)positives.size();
	}
	
	/**
	 * @param positives is the data we want to calculate the F1 of.
	 * @return the F1 score.
	 */
	public Double calculateF1(List<String> positives) {
		double precision = precision(positives);
		double recall = recall(positives);
		
		return 2 * (precision * recall) / (precision + recall);
	}
	
	private void eraseDebugFile(File debugFile) {
		if(this.debug) {
			try {
				Files.write("", debugFile, StandardCharsets.UTF_8);
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void appendRecallDebug(String url) {
		if(this.debug) {
			try {
				Files.append(url + "\n", this.debugRecallFile, StandardCharsets.UTF_8);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void appendPrecisionDebug(String url) {
		if(this.debug) {
			try {
				Files.append(url + "\n", this.debugPrecisionFile, StandardCharsets.UTF_8);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
