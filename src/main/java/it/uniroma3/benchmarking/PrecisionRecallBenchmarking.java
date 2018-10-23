package it.uniroma3.benchmarking;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;

import it.uniroma3.analysis.Combined;
import it.uniroma3.analysis.LinkAbsoluteTransiency;
import it.uniroma3.analysis.LinkMotion;
import scala.Tuple2;

/**
 * Benchmarking class to get Precision & Recall values for all the analysis.
 * @author Luigi D'Onofrio
 */
//TODO: Less staticness
public class PrecisionRecallBenchmarking {
	private static final Logger logger = Logger.getLogger(PrecisionRecallBenchmarking.class);
	private static QualityMeasures measures;

	public static void execute() throws IOException {
		measures = new QualityMeasures(Files.readAllLines(Paths.get("/home/luigi/Desktop/golden")));
		//printStabilityStatistics(0.8);
		//printIntraPageStaticityStatistics(1);
		printCombined(0.1);
	}
	
	private static void printStabilityStatistics(double threshold) {
		double stabiltyThreshold = threshold;
		JavaRDD<Document> stability = (new LinkAbsoluteTransiency()).analyze(true);
		List<String> stabilityData = stability.map(doc -> new Tuple2<>(doc.getString("url"), doc.getDouble("stability")))
										      .filter(tuple -> (tuple._2 >= stabiltyThreshold))
										      .map(tuple -> tuple._1)
										      .distinct()
										      .collect();
		logger.info("Stability PRECISION: " + measures.calculatePrecision(stabilityData));
		logger.info("Stability RECALL: " + measures.calculateRecall(stabilityData));
	}
	
	private static void printIntraPageStaticityStatistics(int threshold) {
		JavaRDD<Document> ipsRDD = (new LinkMotion()).analyze(true);
		List<String> ipsData = ipsRDD.map(doc -> new Tuple2<>(doc.getString("url"), doc.getInteger("score")))
										      .filter(tuple -> (tuple._2 < threshold))
										      .map(tuple -> tuple._1)
										      .distinct()
										      .collect();
		
		logger.info("IPS PRECISION: " + measures.calculatePrecision(ipsData));
		logger.info("IPS RECALL: " + measures.calculateRecall(ipsData));
	}
	
	private static void printCombined(double threshold) {
		List<String> combinedData = (new Combined()).analyze(true)
				   									.mapToPair(doc -> new Tuple2<>(doc.getString("url"), doc.getDouble("score")))
													.filter(tuple -> tuple._2 < threshold)
													.map(x -> x._1)
													.distinct()
													.collect();
		
		logger.info("Combined PRECISION: " + measures.calculatePrecision(combinedData));
		logger.info("Combined RECALL: " + measures.calculateRecall(combinedData));
	}
}
