package it.uniroma3.crawling;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_DEPTH;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_FILE_TO_WRITE_ON;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_NUM_CRAWLERS;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_SEEDS;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_STORAGE;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_WRITE_ON_FILE;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.Arch;
import it.uniroma3.persistence.GraphsDAO;
import it.uniroma3.properties.PropertiesReader;

/**
 * This class is intended for crawling a Website and generating a graph out of it.
 * The graph is modeled by an instance of {@link Graph}.
 * @author Luigi D'Onofrio
 *
 */
public class GraphController {
	private static final Logger logger = Logger.getLogger(GraphController.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final File seeds = new File("./resources/" + propsReader.getProperty(CRAWLER_SEEDS));
	private static final File fileToWriteOn = new File(propsReader.getProperty(CRAWLER_FILE_TO_WRITE_ON));
	private static final String storage = propsReader.getProperty(CRAWLER_STORAGE);
	private static final int numCrawlers = Integer.parseInt(propsReader.getProperty(CRAWLER_NUM_CRAWLERS));
	private static final int maxDepth = Integer.parseInt(propsReader.getProperty(CRAWLER_DEPTH));
	private static final boolean writeOnFile = Boolean.parseBoolean(propsReader.getProperty(CRAWLER_WRITE_ON_FILE));
	private static final boolean writeOnDB = false;

	/**
	 * Crawls a website from a given url and maximum depth, provided both in the configuration file.
	 * The configuration file is 'application.properties'. 
	 * @return an instance of {@link Graph} representing the graph.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static Graph crawlAndBuild() throws Exception {
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(storage);
		config.setMaxDepthOfCrawling(maxDepth);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */
		Files.readLines(seeds, StandardCharsets.UTF_8)
			 .stream()
			 .forEach(seed -> controller.addSeed(seed));
		
		Graph graph = new Graph();

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		controller.start(SpiderCrawler.class, numCrawlers);
		controller.getCrawlersLocalData().stream()
										 .map(x -> (Set<Arch>) x)
										 .flatMap(set -> set.stream())
										 .forEach(arch -> {
											 		graph.addArch(arch);
											 		if(writeOnDB)
											 			GraphsDAO.getInstance().addRelationship(arch.getFromNode(), arch.getToNode());
											 		if(writeOnFile) {
													try {
														Files.append(arch.toCouple(), fileToWriteOn, StandardCharsets.UTF_8);
														Files.append("\n", fileToWriteOn, StandardCharsets.UTF_8);
													}catch(Exception e) {
														logger.error("Error while writing on file!");
													}
										 }});
		GraphsDAO.getInstance().close();
		return graph;

	}
}