package it.uniroma3.crawling.webarchive;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_DEPTH;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_NUM_CRAWLERS;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_SEEDS;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_STORAGE;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import it.uniroma3.crawling.webarchive.utils.SnapshotCollector;
import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.properties.PropertiesReader;

/**
 * This class is intended for crawling a Website to retrieves URLs and Link Occurrences.
 * @author Luigi D'Onofrio
 *
 */
public class WebArchiveCrawlingDriver {
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(WebArchiveCrawlingDriver.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final File SEEDS = new File("./src/main/resources/" + propsReader.getProperty(CRAWLER_SEEDS));
	private static final String STORAGE = propsReader.getProperty(CRAWLER_STORAGE);
	private static final int NUM_CRAWLERS = Integer.parseInt(propsReader.getProperty(CRAWLER_NUM_CRAWLERS));
	private static final int MAX_DEPTH = Integer.parseInt(propsReader.getProperty(CRAWLER_DEPTH));
	private static Pattern pattern = Pattern.compile("^(?:https?:\\/\\/)?(?:[^@\\/\\n]+@)?(?:www\\.)?([^:\\/\\n]+)");
	private static String domain;
	private static final CrawlConfig config = new CrawlConfig();
	private static final PageFetcher pageFetcher = new PageFetcher(config);
	private static WebArchiveCrawlingDriver instance;
	private CrawlController myController;
	private static final RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
	static {
		config.setCrawlStorageFolder(STORAGE);
		config.setMaxDepthOfCrawling(MAX_DEPTH);
		robotstxtConfig.setEnabled(false);
	}
	private static final RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);

	/**
	 * Crawls a website from a given url and maximum depth, provided both in the configuration file.
	 * The configuration file is 'application.properties'. 
	 * @throws Exception
	 */
	private void crawl(String url) throws Exception {
		/*
		 * Instantiate the controller for this crawl.
		 */
		this.myController = new CrawlController(config, pageFetcher, robotstxtServer);
		this.myController.addSeed(url);
		
		MySQLRepositoryDAO.getInstance().incrementSequence();
		
		WebArchiveCrawlerFactory factory = new WebArchiveCrawlerFactory(domain);
		this.myController.start(factory, NUM_CRAWLERS);

	}

	public void crawlAll(String url) throws Exception {	
		if(domain == null) {
			Matcher matcher = pattern.matcher(url);
			if(matcher.find()) {
				domain = matcher.group(0);
			} else
				throw new Exception("No domain found in URL");
		}
		
		List<String> snapshots = (new SnapshotCollector(url)).getAllSnapshotsURLs();
		
		for(String snapshotURL : snapshots) {
			crawl(snapshotURL);
		}
	}
	
	public void crawlAllInRange(String url, Date from, Date to) throws Exception {	
		if(domain == null) {
			Matcher matcher = pattern.matcher(url);
			if(matcher.find()) {
				domain = matcher.group(0);
			} else
				throw new Exception("No domain found in URL");
		}
		
		List<String> snapshots = (new SnapshotCollector(url)).getSnapshotsInRange(from, to);
		for(String snapshotURL : snapshots) {
			crawl(snapshotURL);
		}
	}
	
	

	public static WebArchiveCrawlingDriver getInstance() throws Exception {
		return (instance == null) ? instance = new WebArchiveCrawlingDriver() : instance;
	}
}