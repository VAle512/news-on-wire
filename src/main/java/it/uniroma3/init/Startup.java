package it.uniroma3.init;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import it.uniroma3.analysis.StabilityAnalysis;
import it.uniroma3.crawling.CrawlingDriver;
import it.uniroma3.crawling.SpiderCrawler;
import it.uniroma3.properties.PropertiesReader;

public class Startup {
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	private static final long TIME_TO_WAIT = Long.parseLong(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));
	
	public static void main(String[] args) throws Exception {
		for (int i = 0; i < 14; ++i) {
			CrawlingDriver.crawl();
			logger.info("Waiting for " + TIME_TO_WAIT + " " + TIME_UNIT.toString().toLowerCase() + ".");
			Thread.sleep(TimeUnit.MILLISECONDS.convert(TIME_TO_WAIT, TIME_UNIT));
		}	
	}

}
