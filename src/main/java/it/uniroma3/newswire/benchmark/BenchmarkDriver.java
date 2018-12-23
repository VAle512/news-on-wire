package it.uniroma3.newswire.benchmark;

import static it.uniroma3.newswire.utils.EnvironmentVariables.goldens;
import static org.apache.log4j.Level.INFO;
import static org.apache.log4j.Level.WARN;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.Lists;

import it.uniroma3.newswire.benchmark.utils.MetricsCalculator;
import it.uniroma3.newswire.classification.features.Feature;
import it.uniroma3.newswire.classification.features.PageHyperTextualReferencesDinamicity;
import it.uniroma3.newswire.classification.features.HyperTextualContentDinamycityPlusStability;
import it.uniroma3.newswire.classification.features.Stability;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import scala.Tuple2;

/**
 * Benchmarking class to get Precision & Recall values for all the benchmarks.
 * @author Luigi D'Onofrio
 */
public class BenchmarkDriver {
	private static Logger logger = Logger.getLogger(BenchmarkDriver.class);
	private Map<String,List<Tuple2<Feature, MetricsCalculator>>> db2benchmarks;
	private Map<DAO, List<BenchmarkResult>> dao2results;
	private static int latestSnapshot;
	
	/**
	 * Constructor.
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public BenchmarkDriver() throws InstantiationException, IllegalAccessException, IOException {
		setUp();
		this.dao2results = new HashMap<>();
	}
	
	/**
	 * Sets up all. 
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void setUp() throws IOException, InstantiationException, IllegalAccessException {
		init();	
	}
	
	/**
	 * This method initializes the benchmark suite. If you want to add/remove benchmarks, for now, u have to work here.
	 */
	private void init() {		
		this.db2benchmarks = new HashMap<>();
		
		DAOPool.getInstance().getDatabasesDAOsByName().forEach(dbName -> {
			List<Tuple2<Feature, MetricsCalculator>> db2BenchmarksMetrics = new ArrayList<>();
			
			Stability stability = new Stability(dbName);
			PageHyperTextualReferencesDinamicity hyperTextualContentDinamicity = new PageHyperTextualReferencesDinamicity(dbName);
			HyperTextualContentDinamycityPlusStability hyperTextualContentDinamicitypPlusStability = new HyperTextualContentDinamycityPlusStability(dbName);
			
			File goldenData = null;
			
			try {
				goldenData = getGoldenFileName(dbName);
			} catch (Exception e) {
				logger.warn("No golden provided for " + dbName + ", skipping.");
				return;
			}
			
			List<String> goldenEntries = new ArrayList<>();
			
			try {
				goldenEntries = Files.readAllLines(Paths.get(goldenData.getAbsolutePath()));
			} catch(IOException e) {
				logger.error(e.getMessage());
			}
			
			db2BenchmarksMetrics.add(new Tuple2<Feature, MetricsCalculator>(stability, new MetricsCalculator(dbName, stability, goldenEntries, false)));
			db2BenchmarksMetrics.add(new Tuple2<Feature, MetricsCalculator>(hyperTextualContentDinamicity, new MetricsCalculator(dbName, hyperTextualContentDinamicity, goldenEntries, false)));
			db2BenchmarksMetrics.add(new Tuple2<Feature, MetricsCalculator>(hyperTextualContentDinamicitypPlusStability, new MetricsCalculator(dbName, hyperTextualContentDinamicitypPlusStability, goldenEntries, false)));
			
			//FIXME: Must be fully implemented; we need more data.
			//dbBenchmarks.add(new LinkDinamicityFactor(dbName));
			
			db2benchmarks.put(dbName, db2BenchmarksMetrics);
		});
	}
	
	/**
	 * Executes the entire suite of Benchmarks, involving all entities registered before the provided snapshots.
	 * @param snapshot is the least snapshot we want to include for the scoring.
	 * @param persist should be true if we want to save intermediate results onto the db, false othewise.
	 */
	private void executeBenchmarksAt(DAO dao, int snapshot, boolean persist, List<Feature> benchmarks) {

		String dbName = dao.getDatabaseName();
		List<Feature> dbBenchmarks = db2benchmarks.get(dbName).stream()
																.map(tuple -> tuple._1)
																.filter(benchmark -> benchmarks.contains(benchmark))
																.collect(Collectors.toList());

		/* Check if we actually have that snapshot for the selected DAO. */
		boolean needLastSnapshot = false;

		if(dao.getCurrentSequence() < snapshot) {
			log(INFO, dao, "Selected DAO doesn't have " + snapshot + " snapshot.");
			needLastSnapshot = true;
		}

		/* If don't we swap the snapshot with the latest for the dao.
		 * This will cause duplicates in benchmarks, but we can save us witha  distinct query.	
		 */
		final int _snapshot = (needLastSnapshot) ? dao.getCurrentSequence() : snapshot;

		/*... Execute all benchamarks... */
		dbBenchmarks.forEach(benchmark -> {

			log(INFO, dao, "Started " + benchmark.getBenchmarkSimpleName() + " benchmark...");
			String benchmarkName = benchmark.getCanonicalBenchmarkName();

			/* If the benchmark table doesn't exists create it! */
			boolean benchmarkTableExists = dao.checkTableExists(benchmarkName);
			if(!benchmarkTableExists)
				dao.createBenchmarkTable(benchmarkName);

			/* Calculate the scores for the particular benchmark */
			JavaPairRDD<String, Double> scores = benchmark.calculate(persist, _snapshot);

			Double benchmarkMaxValue = scores.mapToDouble(x -> x._2).max();

			double neighborHoodFactor = (benchmark.getCanonicalBenchmarkName().equals(PageHyperTextualReferencesDinamicity.class.getSimpleName()+"Benchmark")) ? 1. : benchmarkMaxValue / 1000.;
			
			System.out.println("Si ma: " + dbName);
			MetricsCalculator dbQualityMeasures = this.db2benchmarks.get(dbName).stream()
																				.filter(b -> b._1.equals(benchmark))
																				.findFirst()
																				.get()
																				._2();

			try {
				/* find the threshold that maximizes the outcoming (F1-Score) */
				BenchmarkResult result = BenchmarkThresholdFinder.find(benchmark, scores, .0, dbQualityMeasures, neighborHoodFactor, benchmarkMaxValue, _snapshot);
				addResult(dao, result);
			} catch (InstantiationException | IllegalAccessException | IOException e) {
				logger.error(e.getMessage());
			}

			log(INFO, dao, "Ended " + benchmark.getBenchmarkSimpleName() + " benchmark.");
		});	
	}
	
	/**
	 * Executes the entire benchmarks suite for all the stored websites at a specific snapshot.
	 * @param snapshot is the snapshot we want to execute the benchmarks at.
	 * @param persist sholud be true if we want to persist the results onto the DB.
	 */
	public void executeAllBenchmarksAllDAOsAt(int snapshot, boolean persist) {	
		DAOPool.getInstance().getDatabasesDAOs().forEach(dao -> {
			List<Feature> allBenchmarks = db2benchmarks.get(dao.getDatabaseName()).stream()
																					.map(tuple -> tuple._1)
																					.collect(Collectors.toList());
			executeBenchmarksAt(dao, snapshot, persist, allBenchmarks);
		});
		
	}
	
	/**
	 * Executes the entire benchmarks suite for a specified website 
	 * @param dao is the DAO responsible for the storage of the desiderd website.
	 * @param snapshot is the snapshot we want to execute benchmarks at.
	 * @param persist should be true if we want to persist results onto the DB, false otherwise.
	 */
	public void executeAllBenchmarksAt(DAO dao, int snapshot, boolean persist) {
		List<Feature> allBenchmarks = db2benchmarks.get(dao.getDatabaseName()).stream()
																				.map(tuple -> tuple._1)
																				.collect(Collectors.toList());
		executeBenchmarksAt(dao, snapshot, persist, allBenchmarks);
	}
	
	/**
	 * Erases all previously persisted data about benchmarks, and re-executes the entire suite snapshot by snapshot thus reconstructing the history of the crawling.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeBenchmarksFromTheBeginning(DAO dao, List<Feature> benchmarks) throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
				
		/* Erase previously calculated benhcmark data */
		dao.eraseBenchmarkData();
		
		/* Warning: If a DAO hasn't that snapshot, his latest will be calculated. */
		for(int i = 2; i <= latestSnapshot; ++i) {
			logger.info("Executing Benchmarks until snapshot " + i + "...");
			executeBenchmarksAt(dao, i, true, benchmarks);
		}
			
	}
	
	/**
	 * Erases all previously persisted data about benchmarks for a specific stored website, and re-executes the entire suite snapshot by snapshot thus reconstructing the history of the crawling.
	 * @param dao is the DAO responsible for the access to the desider website data.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeAllBenchmarksFromTheBeginning(DAO dao) throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
				
		/* Erase previously calculated benhcmark data */
		dao.eraseBenchmarkData();
		
		/* Warning: If a DAO hasn't that snapshot, his latest will be calculated. */
		for(int i = 2; i <= latestSnapshot; ++i) {
			logger.info("Executing Benchmark suite until snapshot " + i + "...");
			executeAllBenchmarksAt(dao, i, true);
		}
			
	}
	
	/**
	 * Erases all previously persisted data about benchmarks, and re-executes the entire suite for all the stored websites, snapshot by snapshot thus reconstructing the history of the crawling.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeAllBenchmarksFromTheBeginning() throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
		
		List<DAO> daos = DAOPool.getInstance().getDatabasesDAOs();
		
		/* Erase previously calculated benhcmark data */
		daos.forEach(dao -> dao.eraseBenchmarkData());
		
		/* Warning: If a DAO hasn't that snapshot, his latest will be calculated. */
		for(int i = 2; i <= latestSnapshot; ++i) {
			logger.info("Executing Benchmark suite until snapshot " + i + "...");
			executeAllBenchmarksAllDAOsAt(i, true);
		}
			
	}
	
	/**
	 * Executes all the benchmark suite for the selected range of snapshots for all websites.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeAllBenchmarksAllDAOsInRange(int from, int to) throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
		
		for(int i = from; i <= to; ++i) {
			logger.info("Executing Benchmark suite until snapshot " + i + "...");
			executeAllBenchmarksAllDAOsAt(i, true);
		}
			
	}
	
	/**
	 * Executes all the benchmark suite for the selected range of snapshots for a specified website.
	 * @param dao is the DAO responsible for the access to the stored data of the desired website.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeAllBenchmarksInRange(DAO dao, int from, int to) throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
		
		for(int i = from; i <= to; ++i) {
			logger.info("Executing Benchmark suite until snapshot " + i + "...");
			executeAllBenchmarksAt(dao, i, true);
		}
			
	}
	
	/**
	 * Executes the entire benchmark suite for the latest snapshot.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeAllBenchmarksAllDAOsAtLatestSnapshot() throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();	
		executeAllBenchmarksAllDAOsAt(latestSnapshot, true);
		
	}
	
	
	
	/**
	 * Returns the bane of the golden file associated wqith the given databse name.
	 * @param dbName is the database we want retrieve the golden file of.
	 * @return the golldlen file.
	 */
	private File getGoldenFileName(String dbName) {
		File goldenDir = new File(System.getenv(goldens));
		logger.info("Checking for golden: " + dbName);
		return Arrays.asList(goldenDir.listFiles()).stream()
											  	   .filter(file -> file.getName().startsWith(dbName))
											  	   .findFirst().get();
	}
	
	/**
	 * Custom logger to distinguish logging between databases.
	 * @param logLevel is"System.in was closed; exiting" the desired log level of the message.
	 * @param dao is the DAO we are logging for.
	 * @param message is the message we want to print.
	 */
	private static void log(Level logLevel, DAO dao, String message) {
		String daoInfo = "[" + dao.getDatabaseName() + "]";
		logger.log(logLevel, daoInfo + " " + message);
	}
	
	@SuppressWarnings("unused")
	private void persistAllResults() {
		dao2results.forEach((dao, results) -> dao.saveResults(results));
	}
	
	private void addResult(DAO dao, BenchmarkResult benchmarkResult) {
		List<BenchmarkResult> results = this.dao2results.get(dao);
		if(results != null) {
			results.add(benchmarkResult);
			log(INFO, dao, benchmarkResult + " added.");
		} else
			this.dao2results.put(dao, Lists.newArrayList(benchmarkResult));
	}

}
