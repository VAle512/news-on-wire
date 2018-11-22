package it.uniroma3.newswire.benchmark.benchmarks;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import scala.Tuple2;

/**
 * This class is intended for calculating the results of the Stability Analysis.
 * This analysis is based on the simple principle that the more a URLs is crawled (into snapshots) the more it is non-transient so it lasts for a long time. 
 * @author Luigi D'Onofrio
 *
 */
public class Stability extends Benchmark {
	private static final long serialVersionUID = -4512543190277681487L;

	/**
	 * Constructor.
	 */
	public Stability(String dbName) {
		this.resultsTable = this.getClass().getSimpleName();
		this.database = dbName;
		init();
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	public JavaPairRDD<String, Double> analyze(boolean persist, int untilSnapshot) {
		
		/* Erase previous Stability Data */
		if(persist) {
			DAO dao = DAOPool.getInstance().getDAO(this.database);
			if(dao.checkTableExists(this.resultsTable))
				dao.cleanTable(this.resultsTable);
			else
				dao.createAnalysisTable(this.resultsTable);
		}

		JavaRDD<Document> rdd = loadData();
		
		if(untilSnapshot > 0)
			rdd = rdd.filter(x -> x.getInteger(snapshot.name()) <= untilSnapshot).cache();
		
		/* Retrieve the current snapshot counter */
		//int latestSnapshot = DAOPool.getInstance().getDAO(this.database).getCurrentSequence();
		
		/* Let's map into (URL, snapshot) */
		JavaPairRDD<String, Double> result = rdd.map(doc -> new Tuple2<String, Integer>(doc.getString(link.name()), doc.getInteger(snapshot.name())))
											    .distinct()
											    
											    /* Group by URL so we have (URL, (s1, s2, s3...)) 
											     * (Actually there's more but that's the basic idea.  
											     */
											    .groupBy(tuple -> tuple._1())
											  
											    /* divide the number of snapshots a URL is crawled in by the total number of snapshots taken. */
											    .mapToPair(group -> new Tuple2<>(group._1 , 
													  							(double) Iterables.asList(group._2).size() / (double) untilSnapshot));

	   /* Persisting results into the DB. */
	   if(persist) {
		   persist(result, resultsTable);
	   }
	   
		return result;
	}
	
	public String getCanonicalBenchmarkName() {
		return Stability.class.getSimpleName()+"Benchmark";
	}
	

	public String getBenchmarkSimpleName() {
		return Stability.class.getSimpleName();
	}
}
