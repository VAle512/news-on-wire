package it.uniroma3.newswire.crawling;

import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_DEPTH;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_NUM_CRAWLERS;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_STORAGE;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_RESET_ALL;
import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import it.uniroma3.newswire.persistence.ConcurrentPersister;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;

/**
 * This class is intended for crawling a Website to retrieves URLs and Link Occurrences.
 * @author Luigi D'Onofrio
 *
 */
public class CrawlingDriver {
	private static final Logger logger = Logger.getLogger(CrawlingDriver.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final String STORAGE = propsReader.getProperty(CRAWLER_STORAGE);
	private static final int NUM_CRAWLERS = Integer.parseInt(propsReader.getProperty(CRAWLER_NUM_CRAWLERS));
	private static final int MAX_DEPTH = Integer.parseInt(propsReader.getProperty(CRAWLER_DEPTH));

	/**
	 * Crawls a web site from a given URL and maximum depth, provided both in the configuration file.
	 * The configuration file is 'application.properties'. 
	 * @throws Exception
	 */
	public static void crawl() throws Exception {
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(STORAGE);
		config.setMaxDepthOfCrawling(MAX_DEPTH);
				
		String configPath = System.getenv(envConfig);
		File seedFile = new File(configPath + "/" + "seeds");
		
		/* Instantiate the controller for this crawl. */	
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
		Files.readLines(seedFile, StandardCharsets.UTF_8)
			 .stream()
			 .forEach(seed -> {
				 controller.addSeed(seed);
				 DAOPool.getInstance().getDAO(URLUtils.domainOf(seed));
			 });

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */

		DAOPool.getInstance().getDatabasesDAOs().forEach(db -> db.incrementSequence());
		ConcurrentPersister concPersister = new ConcurrentPersister();
		concPersister.start();
		
		logger.info("Starting crawler...");
		
		controller.start(() -> new Crawler(concPersister), 32);
		
		/* Shutdown the current crawling session */
		if(controller.isFinished()) // Should be blocking in case of Non-Blocking crawling
			controller.shutdown();
	}
}