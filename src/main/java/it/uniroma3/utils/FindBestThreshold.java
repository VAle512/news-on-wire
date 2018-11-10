package it.uniroma3.utils;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.api.java.JavaPairRDD;

import it.uniroma3.analysis.Combined;
import it.uniroma3.benchmarking.QualityMeasures;
import scala.Tuple2;

public class FindBestThreshold {
	public static void find(JavaPairRDD<String, Double> data, double startingThreshold, QualityMeasures measures) throws InstantiationException, IllegalAccessException {
		double neighborhoodFactor = 0.1;
		double currentThreshold = startingThreshold;

		List<String> combinedData = data.filter(tuple -> tuple._2 <= startingThreshold)
				  .map(x -> x._1)
				  .distinct()
				  .collect();
		
		double precision = measures.calculatePrecision(combinedData);
		double  recall = measures.calculateRecall(combinedData);
		/* Evaluate Vc */
		double f1_Score = 2*(precision * recall)/(precision + recall);
		
		
		int j = 200;
		double T = j;
		
		double top_f1_score = 0.;
		double top_precision = 0.;
		double top_recall = 0.;
		
		do {
			do {
				double temp = 0.;
				do {
					temp = currentThreshold + ThreadLocalRandom.current().nextDouble(-neighborhoodFactor, neighborhoodFactor);
				}while(temp < 0 || temp > 1);

				final double t = temp;
				combinedData = data.filter(tuple -> tuple._2 <= t)
								   .map(x -> x._1)
								   .distinct()
								   .collect();
				
				precision = measures.calculatePrecision(combinedData);
				recall = measures.calculateRecall(combinedData);
				/* Evaluate Vc */
				f1_Score = 2*(precision * recall)/(precision + recall);
				
				if((new Double(f1_Score)).isNaN())
					f1_Score=0.;
				
				if(top_f1_score < f1_Score) {
					top_f1_score = f1_Score;
					top_precision = precision;
					top_recall = recall;
					currentThreshold = t;
				} else if (Random.class.newInstance().nextDouble() < Math.exp((f1_Score - top_f1_score)*1000/j)) {
					top_f1_score = f1_Score;
					top_precision = precision;
					top_recall = recall;
					currentThreshold = t;
				}
			}while((f1_Score - top_f1_score) > 0.02);
				
				T = T-0.7;
				--j;
				
				System.out.println("p: " + top_precision);
				System.out.println("r: " + top_recall);
				System.out.println("f1: " + top_f1_score);
				System.out.println("t: " + currentThreshold);
				System.out.println("T: " + T);
				System.out.println("#############################");
						
			}while(j>0);
			
		System.out.println("Combined PRECISION: " + top_precision);
		System.out.println("Combined RECALL: " + top_recall);
		System.out.println("Combined F1: " + top_f1_score);
		System.out.println("threshold: " + currentThreshold);
		}
}
