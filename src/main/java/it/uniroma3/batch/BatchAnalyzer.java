package it.uniroma3.batch;

import static it.uniroma3.properties.PropertiesReader.BATCH_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.BATCH_TIME_TO_WAIT;
import static it.uniroma3.properties.PropertiesReader.MONGODB_RESULTS_COLLECTION;
import static it.uniroma3.properties.PropertiesReader.SPARK_REPARTITION_LEVEL;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;

import com.google.common.collect.Lists;
import com.mongodb.spark.MongoSpark;

import it.uniroma3.batch.types.EntryURL;
import it.uniroma3.batch.utils.SparkLoader;
import it.uniroma3.persistence.RepositoryDAO;
import it.uniroma3.properties.PropertiesReader;
import scala.Tuple2;

public class BatchAnalyzer {
	private static final Logger logger = Logger.getLogger(BatchAnalyzer.class); 
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final String RESULTS_COLLECTION = propsReader.getProperty(MONGODB_RESULTS_COLLECTION);
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(BATCH_TIMEUNIT));
	/* How much to wait to start a Batch Analysis */
	private static final long TIME_TO_WAIT = TIME_UNIT.toMillis(Long.parseLong(propsReader.getProperty(BATCH_TIME_TO_WAIT)));

	private BatchAnalyzer() {}

	private static void executeBatchAnalysis() {			
			logger.info("Starting Up Batch Analysis...");
			long startTime = System.currentTimeMillis();
			
			JavaRDD<Document> rdd = SparkLoader.getInstance()
											   .loadDataFromMongo()
											   .repartition(Integer.parseInt(propsReader.getProperty(SPARK_REPARTITION_LEVEL)))
											   .cache();
			
			//List<String> snaphotsList = RepositoryDAO.getInstance().getSnapshots();
			List<String> snaphotsList = rdd.map(doc -> new EntryURL(doc))
										   .map(url -> url.getSnapshot())
										   .distinct().collect();
					
			
			JavaRDD<Document> results = rdd.map(doc -> new EntryURL(doc))
										
								  		 .groupBy(url -> url.getUrl())
								  		 .map(x -> new Tuple2<String, List<String>>(x._1, Lists.newArrayList(x._2)
												  											   .stream()
												  											   .map(k -> k.getSnapshot())
												  											   .distinct()
												  											   .collect(Collectors.toList())))
								  		 .map(x -> new Tuple2<String, Float>(x._1, calculateStability(x._2,snaphotsList)))
								  		 .map(x -> new Document().append("url", x._1).append("stability", x._2));

			long endTime = System.currentTimeMillis();
			logger.info("Batch Analysis finished in: " + (endTime - startTime)/1000 + " seconds.");
			logger.info("Storing results...");
			RepositoryDAO.getInstance().dropDB(RESULTS_COLLECTION);
			MongoSpark.save(results);
			logger.info("Storing results: complete. Stored records: " + results.count());
		
	}
	
	private static Float calculateStability(List<String> urlSnapshots, List<String> allSnaphots) {
		int count = 0;
		
		for(String snapshot: urlSnapshots)
			if(allSnaphots.contains(snapshot))
				++count;
		
		return new Float(((float)count)/ ((float) (allSnaphots.size())));
	}
	
	public static void runInANewThread() {
		new Thread(() ->{
			while(true) {
				try {
					Thread.sleep(TIME_TO_WAIT);
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}

				executeBatchAnalysis();
			}
		}).start();
	}
	
	public static void runOneInstance() {
		executeBatchAnalysis();
	}
}
