package it.uniroma3.streaming.crawling;

import static it.uniroma3.properties.PropertiesReader.GATHERER_WRITE_ON_FILE;
import static it.uniroma3.properties.PropertiesReader.PAGE_RANK_GRAPH_GENERATOR_FILE;

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
import it.uniroma3.graphs.OrientedArch;
import it.uniroma3.pagerank.GraphGenerator;
import it.uniroma3.pagerank.PageRank;
import it.uniroma3.properties.PropertiesReader;

public class CrawlerController {
	private static final Logger logger = Logger.getLogger(GraphGenerator.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final boolean writeOnFile = Boolean.parseBoolean(propsReader.getProperty(GATHERER_WRITE_ON_FILE));
	private static final File file = new File(propsReader.getProperty(PAGE_RANK_GRAPH_GENERATOR_FILE));
	private static Graph graph;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		int numberOfCrawlers = 4;

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder("/home/luigidonofrio/Scrivania/crawler/root");
		config.setMaxDepthOfCrawling(1);

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
		String seed = "http://www.ansa.it";
		controller.addSeed(seed);
		graph = new Graph(seed);

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		controller.start(SpiderCrawler.class, numberOfCrawlers);
		controller.getCrawlersLocalData().stream()
										 .map(x -> (Set<OrientedArch>) x)
										 .flatMap(set -> set.stream())
										 .forEach(arch -> {
											 		graph.addArch(arch);
											 		if(writeOnFile) {
													try {
														Files.append(arch.toCouple(), file, StandardCharsets.UTF_8);
														Files.append("\n", file, StandardCharsets.UTF_8);
													}catch(Exception e) {
														logger.error("Error while writing on file!");
													}
										 }});
		
		PageRank.compute(graph);

	}
}