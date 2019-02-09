package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.benchmark.utils.LinkCollectionFinder.findCollections;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static org.apache.log4j.Level.INFO;

import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.neo4j.driver.internal.util.Iterables;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.spark.SparkLoader;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 * This class is intended to quantify the motion of a link inside a specific page and link collection.
 * in terms of how many xpath changes it counts across all the pages that reference that link.
 * @author Luigi D'Onofrio
 *
 */
public class PageHyperTextualContentDinamicity extends Feature{
	
	/**
	 * Constructor.
	 * @param dbName is the database of the website we are executing the benchmark for.
	 */
	public PageHyperTextualContentDinamicity(String dbName) {
		super(dbName);
	}

	private static final long serialVersionUID = 4965813350830416479L;

	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	@SuppressWarnings("unchecked")
	public JavaPairRDD<String, Double> calculate(boolean persistResults, int untilSnapshot) {
		log(INFO, "started");
		
		JavaPairRDD<String, Double> cached = loadCachedData();
		if(cached !=null)
			if(cached.count() != 0) {
			log(INFO, "Data in cache loaded susccessfully: " + cached.count());
			return cached.cache();
		}
		
		/* Erase previous Stability Data */
		erasePreviousResultsData(persistResults);
		
		/* recalulcate link collections. */
		JavaPairRDD<String, Double> phtRd =  (new PageHyperTextualReferencesDinamicity(this.database)).calculate(persistResults, untilSnapshot);
		JavaPairRDD<Long, Tuple4<String, String, String, Integer>> data = loadData().mapToPair(row -> new Tuple2<>(row.getLong(id.name()),
																							  						 new Tuple4<>(
																							  								 row.getString(link.name()),
																							  								 row.getString(referringPage.name()),
																							  								 row.getString(xpath.name()),
																							  								 row.getInteger(snapshot.name()))
																							  							)
																									).filter(x -> !x._2._1().equals("http://www.nytimes.com/events/"));
		
		if(untilSnapshot > 0)
			data = data.filter(x -> x._2._4() <= untilSnapshot);
		
		JavaPairRDD<String, String> link2referringPage = data.mapToPair(tuple -> new Tuple2<>(tuple._2._1(), tuple._2._2()));
		
		JavaPairRDD<String, Double> referringPage2ContentDinamicity = link2referringPage.join(phtRd)																				
																						.mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2))
																						.groupBy(tuple -> tuple._1) //Grouping by referring page.
																						.mapToPair(tuple -> new Tuple2<>(tuple._1, Lists.newArrayList(tuple._2).stream().mapToDouble(x -> x._2).sum()))
																						.persist(StorageLevel.MEMORY_ONLY_SER());
		
		JavaPairRDD<String,Double> savior = link2referringPage.map(x -> x._1).distinct().subtract(referringPage2ContentDinamicity.map(k->k._1)).mapToPair(x -> new Tuple2<>(x, -1.0));
		System.out.println(link2referringPage.map(x -> x._1).distinct().count());
		System.out.println(referringPage2ContentDinamicity.count());
		
		JavaPairRDD<String, Double> results= savior.union(referringPage2ContentDinamicity);
		
		if(persistResults)
			persist(results);
		
		log(INFO, "ended.");

		
		return results;
	}
	

	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 */
	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score <= threshold;
	}
		
}
