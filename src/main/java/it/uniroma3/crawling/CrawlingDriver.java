package it.uniroma3.crawling;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_DEPTH;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_NUM_CRAWLERS;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_SEEDS;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_STORAGE;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.WebURL;
import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.properties.PropertiesReader;

/**
 * This class is intended for crawling a Website to retrieves URLs and Link Occurrences.
 * @author Luigi D'Onofrio
 *
 */
public class CrawlingDriver {
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(CrawlingDriver.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final File SEEDS = new File("./src/main/resources/" + propsReader.getProperty(CRAWLER_SEEDS));
	private static final String STORAGE = propsReader.getProperty(CRAWLER_STORAGE);
	private static final int NUM_CRAWLERS = Integer.parseInt(propsReader.getProperty(CRAWLER_NUM_CRAWLERS));
	private static final int MAX_DEPTH = Integer.parseInt(propsReader.getProperty(CRAWLER_DEPTH));

	/**
	 * Crawls a website from a given url and maximum depth, provided both in the configuration file.
	 * The configuration file is 'application.properties'. 
	 * @throws Exception
	 */
	public static void crawl() throws Exception {
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(STORAGE);
		config.setMaxDepthOfCrawling(MAX_DEPTH);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */
		Files.readLines(SEEDS, StandardCharsets.UTF_8)
			 .stream()
			 .forEach(seed -> controller.addSeed(seed));

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		MySQLRepositoryDAO.getInstance().incrementSequence();
		controller.start(SpiderCrawler.class, NUM_CRAWLERS);
	}
}