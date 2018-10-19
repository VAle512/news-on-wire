package it.uniroma3.init;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;

import it.uniroma3.analysis.CombinedAnalysis;
import it.uniroma3.analysis.IntraPageStaticityAnalysis;
import it.uniroma3.analysis.StabilityAnalysis;
import it.uniroma3.benchmarking.QualityMeasures;
import it.uniroma3.crawling.CrawlingDriver;
import it.uniroma3.crawling.SpiderCrawler;
import it.uniroma3.properties.PropertiesReader;
import scala.Tuple2;

public class Startup {
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	private static final long TIME_TO_WAIT = Long.parseLong(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));
	private static QualityMeasures measures;
	public static void main(String[] args) throws Exception {
		 measures = new QualityMeasures(Files.readAllLines(Paths.get("/home/luigi/Desktop/golden")));
		 
		for (int i = 0; i < 14; ++i) {
			CrawlingDriver.crawl();
			
			printStabilityStatistics(0.8);
			printIntraPageStaticityStatistics(1);
			//printCombined(0.2);
			
			logger.info("Waiting for " + TIME_TO_WAIT + " " + TIME_UNIT.toString().toLowerCase() + ".");
			Thread.sleep(TimeUnit.MILLISECONDS.convert(TIME_TO_WAIT, TIME_UNIT));
		}	
	}
	
	private static void printStabilityStatistics(double threshold) {
		double stabiltyThreshold = threshold;
		JavaRDD<Document> stability = StabilityAnalysis.analyze();
		List<String> stabilityData = stability.map(doc -> new Tuple2<>(doc.getString("url"), doc.getDouble("stability")))
										      .map(tuple -> (tuple._2 < stabiltyThreshold) ? null : tuple)
										      .filter(x -> x != null)
										      .map(tuple -> tuple._1)
										      .collect();
		System.out.println("PRECISION: " + measures.calculatePrecision(stabilityData));
		System.out.println("RECALL: " + measures.calculateRecall(stabilityData));
	}
	
	private static void printIntraPageStaticityStatistics(int threshold) {
		int IPSThreshold = threshold;
		JavaRDD<Document> ipsRDD = IntraPageStaticityAnalysis.analyze();
		List<String> ipsData = ipsRDD.map(doc -> new Tuple2<>(doc.getString("url"), doc.getInteger("ips")))
										      .map(tuple -> (tuple._2 >= IPSThreshold) ? null : tuple)
										      .filter(x -> x != null)
										      .map(tuple -> tuple._1)
										      .distinct()
										      .collect();
		
		System.out.println("PRECISION: " + measures.calculatePrecision(ipsData));
		System.out.println("RECALL: " + measures.calculateRecall(ipsData));
	}
	
	private static void printCombined(double threshold) {
		double combinedThreshold = threshold;
		JavaPairRDD<String, Double> combined = CombinedAnalysis.analyze();
		List<String> combinedData = combined.map(tuple -> (tuple._2 > combinedThreshold) ? null : tuple)
											.filter(x -> x != null)
											.map(x -> x._1)
											.collect();
		
		System.out.println(measures.calculatePrecision(combinedData));
		System.out.println(measures.calculateRecall(combinedData));
	}

}
