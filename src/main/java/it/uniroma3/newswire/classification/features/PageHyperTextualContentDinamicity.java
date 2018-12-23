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
		
		/* Erase previous Stability Data */
		erasePreviousBenchmarkData(persistResults);
		
		/* recalulcate link collections. */
		JavaPairRDD<String, Double> phtRd =  (new PageHyperTextualReferencesDinamicity(this.database)).calculate(persistResults, untilSnapshot);
		JavaPairRDD<Integer, Tuple4<String, String, String, Integer>> data = loadData().mapToPair(row -> new Tuple2<>(row.getInteger(id.name()),
																							  						 new Tuple4<>(
																							  								 row.getString(link.name()),
																							  								 row.getString(referringPage.name()),
																							  								 row.getString(xpath.name()),
																							  								 row.getInteger(snapshot.name()))
																							  							)
																									);
		
		if(untilSnapshot > 0)
			data = data.filter(x -> x._2._4() <= untilSnapshot);
		
		JavaPairRDD<String, String> link2referringPage = data.mapToPair(tuple -> new Tuple2<>(tuple._2._1(), tuple._2._2()));
		
		JavaPairRDD<String, Double> referringPage2ContentDinamicity = link2referringPage.join(phtRd)
																						.mapToPair(tuple -> new Tuple2<>(tuple._2._1, tuple._2._2))
																						.groupBy(tuple -> tuple._1) //Grouping by referring page.
																						.mapToPair(tuple -> new Tuple2<>(tuple._1, Lists.newArrayList(tuple._2).stream().mapToDouble(x -> x._2).sum()));
		if(persistResults)
			persist(referringPage2ContentDinamicity);
		
		log(INFO, "ended.");

		
		return referringPage2ContentDinamicity;
	}
	

	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 */
	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score <= threshold;
	}
		
}
