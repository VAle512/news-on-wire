package it.uniroma3.init;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;

import it.uniroma3.analysis.Combined;
import it.uniroma3.analysis.LinkMotion;
import it.uniroma3.analysis.LinkAbsoluteTransiency;
import it.uniroma3.benchmarking.PrecisionRecallBenchmarking;
import it.uniroma3.benchmarking.QualityMeasures;
import it.uniroma3.crawling.CrawlingDriver;
import it.uniroma3.crawling.SpiderCrawler;
import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.properties.PropertiesReader;
import scala.Tuple2;

public class Startup {
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	private static final long TIME_TO_WAIT = Long.parseLong(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));
	
	public static void main(String[] args) throws Exception {	 
		//MySQLRepositoryDAO.getInstance().resetAll();
		for (int i = 0; i < 14; ++i) {
			logger.info("Staring crawling phase...");
			//CrawlingDriver.crawl();
			PrecisionRecallBenchmarking.execute();
			System.exit(0);
			logger.info("Waiting for " + TIME_TO_WAIT + " " + TIME_UNIT.toString().toLowerCase() + ".");
			Thread.currentThread().sleep(TimeUnit.MILLISECONDS.convert(TIME_TO_WAIT, TIME_UNIT));
		}	
	}

}
