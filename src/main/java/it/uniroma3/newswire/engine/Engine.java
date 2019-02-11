package it.uniroma3.newswire.engine;

import static it.uniroma3.newswire.crawling.GenericCrawlingDriver.crawl;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_DEPTH;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;
import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;
import static it.uniroma3.newswire.utils.EnvironmentVariables.goldens;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import it.uniroma3.newswire.classification.features.TextToBodyRatio;
import it.uniroma3.newswire.cli.CLI;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;

public class Engine {
	private final static Logger logger = Logger.getLogger(Engine.class);
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private static int timeToWait = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));
	private static final TimeUnit TIME_TO_WAIT_TIMEUNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	private static final double FRESHNESS_THRESHOLD = 0.4;


	/*
	 * Just during developing this method has a signature with a String passed as parameter.
	 */
	public void start(String siteRoot, int depth) throws Exception {
//		int maxDepth = Integer.parseInt(propsReader.getProperty(CRAWLER_DEPTH));
//		int currentDepth = 0;
//		logger.info("Crawling started...");
//		
//		boolean resetAll = true;
//		/* Personal protection. */
////		boolean resetAll = Boolean.parseBoolean(propsReader.getProperty(MYSQL_RESET_ALL));
//		/* Never erase anything resume everytime */
//		
//		checkGoldens();
//		
//		init(resetAll);
//		while(true) {
//			/*
//			 * CRAWLING PHASE AT A CERTAIN DEPTH 
//			 */
//			double freshness = crawl(siteRoot, depth);
//			logger.info("Waiting for " + timeToWait + " " + TIME_TO_WAIT_TIMEUNIT.toString().toLowerCase());
//			
//			
//			/*
//			 * CRAWLING EVALUATION PHASE
//			 * the idea is to take from the crawler the list of all the pages already present at a certain depth and do a ratio with the total of the pages. 
//			 */
//		
//			if(freshness < FRESHNESS_THRESHOLD) {
//				depth = Math.min(depth + 1, 2);
//				logger.info("new Depth: " + depth);
//			}
//			
//			Thread.currentThread().sleep(TIME_TO_WAIT_TIMEUNIT.toMillis(timeToWait));

			/*
			 * FEATURES EXTRACTION
			 * Extract the features two kinds: those who needs reset and those not.
			 */
//			TextToBodyRatio f = new TextToBodyRatio("ansa_it");
//			f.calculate(false, 1);
			/*
			 * CLASSIFICATION
			 * Let's temporary tag our pages.
			 */
//		}
	}
	
	private static void checkGoldens() {
		File goldenDir = new File(System.getenv(goldens));
		List<String> databases = DAOPool.getInstance().getDatabasesDAOsByName();

		for(String database : databases)
			if(Arrays.asList(goldenDir.list()).contains(database + ".csv"))
				logger.info(database + ": golden loaded successfully.");
			else
				logger.error(database + ": golden not found.");
	}

	protected static void init(boolean resetAll) throws IOException {
		String configPath = System.getenv(envConfig);
		File seedFile = new File(configPath + File.separator + "seeds");
		
		//TODO: Rivedere responsabilità e anche praticità
		//TODO: Aggiungere l'eliminazione anche dei file, controllare responsabilità.
		if(resetAll)
			Files.readLines(seedFile, StandardCharsets.UTF_8)
				 .stream()
				 .forEach(seed -> DAOPool.getInstance().getDAO(URLUtils.getDatabaseNameOf(seed)).resetData());	
			
		DAOPool.getInstance().loadAllDAOs();
		
	}
}
