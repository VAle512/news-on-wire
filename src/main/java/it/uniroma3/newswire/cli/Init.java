package it.uniroma3.newswire.cli;

import static it.uniroma3.newswire.cli.CLI.showCLI;
import static it.uniroma3.newswire.crawling.CrawlingDriver.crawl;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_RESET_ALL;
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

import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;

public class Init {
	private static final String FORCE_CRAWLING_PROPERTY = "force-crawling"; 
	private static final String USE_CLI_PROPERTY = "cli";
	
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private static Logger logger = Logger.getLogger(CLI.class);
	private static int timeToWait = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));

	private static final TimeUnit TIME_TO_WAIT_TIMEUNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	
	public static void main(String[] args) throws Exception {
		/* Check for VM arguments */
		boolean forceCrawlOnly = Boolean.parseBoolean(System.getProperty(FORCE_CRAWLING_PROPERTY));
		boolean useCLI = Boolean.parseBoolean(System.getProperty(USE_CLI_PROPERTY));
		
		if(forceCrawlOnly)
			doCrawl();
		else if (useCLI)
			showCLI();
		
	}
	
	@SuppressWarnings("static-access")
	private static void doCrawl() throws Exception {
		logger.info("Crawling started...");
		boolean resetAll = Boolean.parseBoolean(propsReader.getProperty(MYSQL_RESET_ALL));
		checkGoldens();
		
		init(resetAll);
		
		while(true) {
			crawl();
			Thread.currentThread().sleep(TIME_TO_WAIT_TIMEUNIT.toMillis(timeToWait));
		}
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

		if(resetAll)
			Files.readLines(seedFile, StandardCharsets.UTF_8)
				 .stream()
				 .forEach(seed -> DAOPool.getInstance().getDAO(URLUtils.domainOf(seed)).resetData());	
			
		Files.readLines(seedFile, StandardCharsets.UTF_8)
			 .stream()
			 .forEach(seed -> DAOPool.getInstance().getDAO(URLUtils.domainOf(seed)));
		
	}
}
