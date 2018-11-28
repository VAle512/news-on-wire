package it.uniroma3.newswire.cli;

import static it.uniroma3.newswire.crawling.CrawlingDriver.crawl;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_RESET_ALL;
import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;
import static it.uniroma3.newswire.utils.EnvironmentVariables.goldens;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import it.uniroma3.newswire.benchmark.BenchmarkDriver;
import it.uniroma3.newswire.benchmark.utils.QualityMeasuresCalculator;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;
import jersey.repackaged.com.google.common.collect.Lists;

public class CLI {
	private static final boolean useCLI= Boolean.parseBoolean(System.getProperty("cli"));
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private static Logger logger = Logger.getLogger(CLI.class);
	private static int timeToWait = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));

	private static final TimeUnit TIME_TO_WAIT_TIMEUNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));


	public static void main(String[] args) throws Exception {
		boolean resetAll = Boolean.parseBoolean(propsReader.getProperty(MYSQL_RESET_ALL));

		checkGoldens();

		Scanner scanner = new Scanner(System.in);

		if(useCLI) {
			try {
				System.out.println("Welcome " + System.getProperty("user.name") + "!");
				showChoices();

				int choice = scanner.nextInt();

				if(choice == 1) {
					resetAll = false;
					init(resetAll);

					while(true) {
						crawl();
						logger.info("Waiting for " + timeToWait + " " + TIME_TO_WAIT_TIMEUNIT.toString() + ".");   		
						Thread.sleep(TimeUnit.MILLISECONDS.convert(timeToWait, TIME_TO_WAIT_TIMEUNIT));
					}
				}

				if(choice == 2) {
					if(resetAll) {
						System.out.println("Are you sure you want to erase all the data? [Y/n]");
						
						scanner.nextLine();
						char l = scanner.nextLine().charAt(0);
						
						if (l != 'Y') {
							System.out.println("Aborting. Change mysql.resetAll=true in application.properties. This is a double security check.");
							System.exit(1);
						}

						System.out.println("type DELETE to confirm: ");
						
						String deleteString = scanner.nextLine();
						
						if(!deleteString.equals("DELETE"))
							throw new IllegalStateException();
						
						init(resetAll);
						while(true) {
							crawl();
							logger.info("Waiting for " + timeToWait + " " + TIME_TO_WAIT_TIMEUNIT.toString() + ".");   		
							Thread.sleep(TimeUnit.MILLISECONDS.convert(timeToWait, TIME_TO_WAIT_TIMEUNIT));
						}
					}		
				}

				if(choice == 3) {
					(new BenchmarkDriver()).executeLatestSnapshot();
				}

				if(choice == 4) {
					(new BenchmarkDriver()).executeFromTheBeginning();
				}

				if(choice == 5) {
					System.out.println("Please insert the snapshot you want to execute the benchmark suite for:");
					int snapshot = scanner.nextInt();
					(new BenchmarkDriver()).executeUntil(snapshot);
				}
				
				if(choice == 6) {
					init(false);
					AtomicInteger i = new AtomicInteger(0);
					
					System.out.println("Please insert the database you want to execute the benchmark suite for:");
					Map<Integer, DAO> choice2dao = DAOPool.getInstance().getDatabasesDAOs().stream().collect(Collectors.toMap(x -> i.incrementAndGet(), x -> x));
					choice2dao.entrySet().forEach(entry -> System.out.println("\t" + entry.getKey() + ". " + entry.getValue().getDatabaseName()));
					
					System.out.println("Your choice: ");
					int snapshot = scanner.nextInt();
					//TODO: Complete this
					(new BenchmarkDriver()).executeUntil(snapshot);
				}
				
			} catch(IllegalStateException | NoSuchElementException e) {
				// System.in has been closed
				System.out.println("System.in was closed; exiting");
			}

		} else {
			if(resetAll) {
				System.out.println("Are you sure you want to erase all the data? [Y/n]");
				int line = scanner.nextInt();

				if (line != 'Y') {
					System.out.println("Aborting. Change mysql.resetAll=true in application.properties. This is a double security check.");
					System.exit(1);
				}
			}	
			init(resetAll);
			while(true) {
				crawl();
				logger.info("Waiting for " + timeToWait + " " + TIME_TO_WAIT_TIMEUNIT.toString() + ".");   		
				Thread.sleep(TimeUnit.MILLISECONDS.convert(timeToWait, TIME_TO_WAIT_TIMEUNIT));
			}
		}
	}

	private static void showChoices() throws IOException {
		init(false);
		int latestSnapshot = DAOPool.getInstance().getDatabasesDAOs().get(0).getCurrentSequence();

		System.out.println("Press the number corresponding to your choice:");
		System.out.println("\t1. Start Crawling from latest snapshot: " + latestSnapshot);
		System.out.println("\t2. Erase all data and start crawling again");
		System.out.println("\t3. Execute the entire benchmark suite for the latest snapshot: " + latestSnapshot);
		System.out.println("\t4. Execute the entire benchmark suite all across snapshots: [0 - " + latestSnapshot + "]");
		System.out.println("\t5. Execute the entire benchmark suite for a specified snapshot");
		System.out.println("\t6. Execute the entire benchmark suite for a specified website");
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
