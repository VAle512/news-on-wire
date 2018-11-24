package it.uniroma3.newswire.init;

import static it.uniroma3.newswire.properties.PropertiesReader.BENCHMARK_REDO_ALL;
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
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import it.uniroma3.newswire.benchmark.BenchmarkDriver;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;

public class CLI {	
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private static Logger logger = Logger.getLogger(CLI.class);
	private static int timeToWait = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));
	
	private static final TimeUnit TIME_TO_WAIT_TIMEUNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	
			
	public static void main(String[] args) throws Exception {
		boolean resetAll = Boolean.parseBoolean(propsReader.getProperty(MYSQL_RESET_ALL));
		init(resetAll);

		logger.info("Started crawling with " + Runtime.getRuntime().availableProcessors() + " CPUs!");
		checkGoldens();
		
		boolean benchmarkRedoAll = Boolean.parseBoolean(propsReader.getProperty(BENCHMARK_REDO_ALL));
		boolean atLeastTwoSnapshots = DAOPool.getInstance().getDatabasesDAOs().get(0).getCurrentSequence() > 1;
		boolean justOneTime = true;
		
		
		Scanner scanner = new Scanner(System.in);
        try {
            if(resetAll) {
            	System.out.println("Are you sure you want to erase all the data? [y/N]");
            	int line = scanner.nextInt();
            		
            	if (line == 'N') {
            		System.out.println("Aborting. Change mysql.resetAll=true in application.properties.");
            		System.exit(1);
            	}
            }		
        		//while(true) {
    			logger.info("Started crawling phase...");
    			
//    			if(atLeastTwoSnapshots && justOneTime)
//    				if(benchmarkRedoAll) 
//    					executeFromTheBeginning();
//    				else
//    					executeLatestSnapshot();
    			
    			//crawl();
    			
//    			if(!justOneTime)
//    			if(atLeastTwoSnapshots)
    			
    			BenchmarkDriver.executeUntil(5);
    			
//    			justOneTime = false;
//    			logger.info("Waiting for " + timeToWait + " " + TIME_TO_WAIT_TIMEUNIT.toString() + ".");
    		
    			//Thread.sleep(TimeUnit.MILLISECONDS.convert(timeToWait, TIME_TO_WAIT_TIMEUNIT));
    		//}	
            	
        } catch(IllegalStateException | NoSuchElementException e) {
            // System.in has been closed
            System.out.println("System.in was closed; exiting");
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
	
	private static void init(boolean resetAll) throws IOException {
		String configPath = System.getenv(envConfig);
		File seedFile = new File(configPath + File.separator + "seeds");
		
		if(resetAll) {
			Files.readLines(seedFile, StandardCharsets.UTF_8)
				 .stream()
				 .forEach(seed -> DAOPool.getInstance().getDAO(URLUtils.domainOf(seed)).resetData());		
		} else {
			Files.readLines(seedFile, StandardCharsets.UTF_8)
			 .stream()
			 .forEach(seed -> DAOPool.getInstance().getDAO(URLUtils.domainOf(seed)));
		}
	}

}