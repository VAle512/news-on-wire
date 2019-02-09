package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static org.apache.log4j.Level.INFO;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

/**
 * Questa classe serve a calcolare la feature Stability. Essa è intesa come la frequenza con cui un certo URL viene pescato durante il crawling.
 * Questa classe è già stata sottoposta ad ottimizzazione.
 * @author Luigi D'Onofrio
 *
 */
public class Stability extends Feature {
	private static final long serialVersionUID = -4512543190277681487L;
	
	/**
	 * Constructor.
	 */
	public Stability(String dbName) {
		super(dbName);
	}

	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	public JavaPairRDD<String, Double> calculate(boolean persistResults, int untilSnapshot) {
		log(INFO, "started");
		/*
		 * Prendo i dati calcolati per lo snapshot precedente.
		 */
		JavaPairRDD<String, Double> previousSnapshotData = loadPreviousSnapshotData();
		JavaPairRDD<String, Double> result;
		/*
		 * Se non ci sono dati siamo di fronte alla prima run.
		 */
		if(previousSnapshotData == null) {
			logger.info("No previous data present: Initializing data structure.");
			
			/* Mappiamo in (URL, snapshot) */
			result = loadDataIncremental(untilSnapshot, false).mapToPair(doc -> new Tuple2<String, Integer>(doc.getString(link.name()), doc.getInteger(snapshot.name())))
														.distinct()
														.mapToPair(tuple -> new Tuple2<String, Integer>(tuple._1, 1))
														/* Gruppiamo per URL avendo (URL, (s1, s2, s3...)) */
														.reduceByKey((t1, t2) -> t1 + t2)									
														/* divide the number of snapshots a URL is crawled in by the total number of snapshots taken. */
														.mapToPair(group -> new Tuple2<>(group._1 , 
																(double) group._2 / (double) untilSnapshot));
														

		} else {
			logger.info("Previous Data found: Using for re-calculation.");
			/*
			 * Prendo l'ultima fettina.
			 */
			JavaPairRDD<String, Integer> latestSlice = loadDataIncremental(untilSnapshot, false).mapToPair(doc -> new Tuple2<>(doc.getString(link.name()), 1)).distinct();
			logger.info("latestSlice: " + latestSlice.count());
			/*
			 * ... ma ci sono delle entries che non sono state pescate, pertanto le metteremo a 0.
			 */
			JavaPairRDD<String, Integer> missedEntries = previousSnapshotData.keys().subtract(latestSlice.keys()).mapToPair(key -> new Tuple2<>(key, 0));
			latestSlice = latestSlice.union(missedEntries);
			logger.info("latestSlice dopo missed: " + latestSlice.count());
			/*
			 * Per quanto riguarda invece le nuove entries gli daremo questo valore.
			 */
			JavaPairRDD<String, Double> newEntries = latestSlice.keys().subtract(previousSnapshotData.keys()).mapToPair(key -> new Tuple2<>(key, 1./ (double)untilSnapshot));
			logger.info("newEntries: " + newEntries.count());
			logger.info("previousData: " + previousSnapshotData.count());
			
			result = previousSnapshotData.join(latestSlice).mapToPair(joinEntry -> {
				String url = joinEntry._1;
				Double previousScore = joinEntry._2._1;
				Integer isPresent = joinEntry._2._2; // Può essere 1 se presente nello snapshot, 0 altriemnti.

				Double newScore = (previousScore * (untilSnapshot - 1) + isPresent) / untilSnapshot;
				return new Tuple2<>(url, newScore);
			});
			
			logger.info("result: " + result.count());
			result = result.union(newEntries);
			logger.info("result: " + result.count());
		}

		/** 
		 * Salviamo i dati sul DB. 
		 * Questa parte è fondametale perchè permette sia di alleggerire il carico sulla RAM che di riprendere l'analisi successivamente.
		 */
		erasePreviousResultsData(true);
		persist(result);

		log(INFO, "ended.");

		return result;
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 * DEPRECATED: A causa del fatto che non usiamo più un modello basato su scoring function.
	 */
	@Override
	@Deprecated
	public boolean isThresholded(Double score, Double threshold) {
		return score >= threshold;
	}

}
