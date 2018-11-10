package it.uniroma3.init;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;

import java.io.File;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import it.uniroma3.analysis.LinkMotion;
import it.uniroma3.benchmarking.PrecisionRecallBenchmarking;
import it.uniroma3.crawling.CrawlingDriver;
import it.uniroma3.crawling.SpiderCrawler;
import it.uniroma3.crawling.webarchive.WebArchiveCrawlingDriver;
import it.uniroma3.crawling.webarchive.utils.SnapshotCollector;
import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.properties.PropertiesReader;

public class Startup {
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	private static final long TIME_TO_WAIT = Long.parseLong(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));
	
	public static void main(String[] args) throws Exception {
//		MySQLRepositoryDAO.getInstance().updateCollections();
//		System.exit(0);
//		MySQLRepositoryDAO.getInstance().resetAll();
//		WebArchiveCrawlingDriver.getInstance().crawlAllInRange("www.ansa.it/sito/notizie/cronaca/cronaca.shtml", new Date(2018-1900, 0,25,0,0,0), new Date(2018-1900,0,27,0,0,0));
//		PrecisionRecallBenchmarking.execute();
//		System.exit(0);
//		MySQLRepositoryDAO.getInstance().printURLsOnFile(new File("/home/luigi/results.out"));
//		System.exit(0);

		for (int i = 0; i < 14; ++i) {
			logger.info("Staring crawling phase...");
			CrawlingDriver.crawl();
			PrecisionRecallBenchmarking.execute();
			logger.info("Waiting for " + TIME_TO_WAIT + " " + TIME_UNIT.toString().toLowerCase() + ".");
			Thread.currentThread().sleep(TimeUnit.MILLISECONDS.convert(TIME_TO_WAIT, TIME_UNIT));
		}	
	}

}
