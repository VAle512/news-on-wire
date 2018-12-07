package it.uniroma3.newswire.benchmark;

import static it.uniroma3.newswire.utils.EnvironmentVariables.goldens;
import static org.apache.log4j.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.collect.Lists;

import it.uniroma3.newswire.benchmark.benchmarks.Benchmark;
import it.uniroma3.newswire.benchmark.benchmarks.HyperTextualContentDinamicity;
import it.uniroma3.newswire.benchmark.benchmarks.HyperTextualContentDinamycityPlusStability;
import it.uniroma3.newswire.benchmark.benchmarks.Stability;
import it.uniroma3.newswire.benchmark.utils.QualityMeasuresCalculator;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;

/**
 * Benchmarking class to get Precision & Recall values for all the benchmarks.
 * @author Luigi D'Onofrio
 */
public class BenchmarkDriver {
	private static Logger logger = Logger.getLogger(BenchmarkDriver.class);
	private Map<String, QualityMeasuresCalculator> db2golden;
	private Map<String,List<Benchmark>> db2benchmarks;
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
		loadBenchmarks();
	}
	
	/**
	 * Executes the entire suite of Benchmarks, involving all entities registered before the provided snapshots.
	 * @param snapshot is the least snapshot we want to include for the scoring.
	 * @param persist should be true if we want to save intermediate results onto the db, false othewise.
	 */
	private void executeToSnapshot(int snapshot, boolean persist) {
		
		/* For each saved web-site... */
		DAOPool.getInstance().getDatabasesDAOs().forEach(dao -> {
			
			String dbName = dao.getDatabaseName();
			List<Benchmark> dbBenchmarks = db2benchmarks.get(dbName);
			
			/*... Execute all benchamarks... */
			dbBenchmarks.forEach(benchmark -> {
				
				log(INFO, dao, "Started " + benchmark.getBenchmarkSimpleName() + " benchmark...");
				String benchmarkName = benchmark.getCanonicalBenchmarkName();
				
				/* If the benchmark table doesn't exists create it! */
				boolean benchmarkTableExists = dao.checkTableExists(benchmarkName);
				if(!benchmarkTableExists)
					dao.createBenchmarkTable(benchmarkName);
				
				/* Calculate the scores for the particular benchmark */
				JavaPairRDD<String, Double> scores = benchmark.analyze(persist, snapshot);

				Double benchmarkMaxValue = scores.mapToDouble(x -> x._2).max();
				
				double neighborHoodFactor = (benchmark.getCanonicalBenchmarkName().equals(HyperTextualContentDinamicity.class.getSimpleName()+"Benchmark")) ? 1. : benchmarkMaxValue / 1000.;
				
				QualityMeasuresCalculator dbQualityMeasures = db2golden.get(dbName);
				
				try {
					/* find the threshold that maximizes the outcoming (F1-Score) */
					BenchmarkResult result = BenchmarkThresholdFinder.find(dbName, benchmark.getCanonicalBenchmarkName(), scores, .0, dbQualityMeasures, neighborHoodFactor, benchmarkMaxValue, snapshot);
					addResult(dao, result);
				} catch (InstantiationException | IllegalAccessException | IOException e) {
					logger.error(e.getMessage());
				}
				
				log(INFO, dao, "Ended " + benchmark.getBenchmarkSimpleName() + " benchmark.");
			});
			
		});		
	}
	
	/**
	 * Erases all previously persisted data about benchmarks, and re-executes the entire suite snapshot by snapshot thus reconstructing the history of the crawling.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeUntil(int snapshot) throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
		
		/* Erase previously calculated benhcmark data */
		DAOPool.getInstance().getDatabasesDAOs().forEach(dao -> dao.eraseBenchmarkData());
		executeToSnapshot(snapshot, true);	
	
	}
	
	/**
	 * Erases all previously persisted data about benchmarks, and re-executes the entire suite snapshot by snapshot thus reconstructing the history of the crawling.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeFromTheBeginning() throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
		
		/* Erase previously calculated benhcmark data */
		DAOPool.getInstance().getDatabasesDAOs().forEach(dao -> dao.eraseBenchmarkData());
		
		for(int i = 2; i <= latestSnapshot; ++i) {
			logger.info("Executing Benchmark suite until snapshot " + i + "...");
			executeToSnapshot(i, false);
		}
			
	}
	
	/**
	 * Executes all the benchmark suite for the selected range of snapshots.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeInRange(int from, int to) throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();
		
		for(int i = from; i <= to; ++i) {
			logger.info("Executing Benchmark suite until snapshot " + i + "...");
			executeToSnapshot(i, false);
		}
			
	}
	
	/**
	 * Executes the entire benchmark suite for the latest snapshot.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 */
	public void executeLatestSnapshot() throws InstantiationException, IllegalAccessException, IOException {
		/* Sets everything up. */
		setUp();	
		executeToSnapshot(latestSnapshot, true);
		
	}
	
	/**
	 * This method initializes some key stuff. 
	 */
	private void init() {
		latestSnapshot = DAOPool.getInstance().getDatabasesDAOs().stream().findFirst().get().getCurrentSequence();
		
		this.db2golden = new HashMap<>();
		
		DAOPool.getInstance().getDatabasesDAOsByName().forEach(db -> {
			File goldenFile = getGoldenFileName(db);
			
			List<String> goldenEntries = new ArrayList<>();
			
			try {
				goldenEntries = Files.readAllLines(Paths.get(goldenFile.getAbsolutePath()));
			} catch(IOException e) {
				logger.error(e.getMessage());
			}
			
			db2golden.put(db, new QualityMeasuresCalculator(goldenEntries));
		});
	}
	
	/**
	 * This method initializes the benchmark suite. If you want to add/remove benchmarks, for now, u have to work here.
	 */
	private void loadBenchmarks() {		
		this.db2benchmarks = new HashMap<>();
		
		DAOPool.getInstance().getDatabasesDAOsByName().forEach(dbName -> {
			List<Benchmark> dbBenchmarks = new ArrayList<>();
			
			dbBenchmarks.add(new Stability(dbName));
			dbBenchmarks.add(new HyperTextualContentDinamicity(dbName));
			dbBenchmarks.add(new HyperTextualContentDinamycityPlusStability(dbName));
			
			//FIXME: Must be fully implemented; we need more data.
			//dbBenchmarks.add(new LinkDinamicityFactor(dbName));
			
			db2benchmarks.put(dbName, dbBenchmarks);
		});
	}
	
	/**
	 * Returns the bane of the golden file associated wqith the given databse name.
	 * @param dbName is the database we want retrieve the golden file of.
	 * @return the golldlen file.
	 */
	private File getGoldenFileName(String dbName) {
		File goldenDir = new File(System.getenv(goldens));
		
		return Arrays.asList(goldenDir.listFiles()).stream()
											  	   .filter(file -> file.getName().startsWith(dbName))
											  	   .findFirst().get();
	}
	
	/**
	 * Custom logger to distinguish logging between databases.
	 * @param logLevel is the desired log level of the message.
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
