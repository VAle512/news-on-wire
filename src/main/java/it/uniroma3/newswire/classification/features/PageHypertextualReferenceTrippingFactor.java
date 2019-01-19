package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static org.apache.log4j.Level.INFO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Lists;

import scala.Tuple2;
import scala.Tuple3;

/**
 * This class is intended to quantify the motion of a link inside a specific page and link collection.
 * in terms of how many xpath changes it counts across all the pages that reference that link.
 * @author Luigi D'Onofrio
 *
 */
public class PageHypertextualReferenceTrippingFactor extends Feature{
	
	/**
	 * Constructor.
	 * @param dbName is the database of the website we are executing the benchmark for.
	 */
	public PageHypertextualReferenceTrippingFactor(String dbName) {
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
		if(persistResults)
			erasePreviousBenchmarkData(persistResults);
		
	
		//FIXME: Debug purposes swapped relative and referringPage
		JavaPairRDD<Long, Tuple3<String, String, Integer>> data = loadData().mapToPair(row -> new Tuple2<>(row.getLong(id.name()),
																							  						 new Tuple3<>(
																							  								 row.getString(link.name()),
																							  								 row.getString(referringPage.name()),
																							  								 row.getInteger(snapshot.name()))
																							  							)
																									);
		if(untilSnapshot > 0)
			data = data.filter(x -> x._2._3() <= untilSnapshot);
		
		JavaPairRDD<String, Double> results = data.groupBy(id2LinkRefSnap -> id2LinkRefSnap._2._1()) //Let's group by link
												  .mapToPair(link2List-> {
													  String link = link2List._1;
													  Set<Tuple2<String, Integer>> lstRefPage2Snapshot = Lists.newArrayList(link2List._2).stream()
															  																		   .map(triple -> new Tuple2<>(triple._2._2(), triple._2._3()))
															  																		   .collect(Collectors.toSet());
													  return new Tuple2<>(link, lstRefPage2Snapshot);
												}).mapToPair(x -> {
													Map<Integer, List<Tuple2<String, Integer>>> snapshot2refs = x._2.stream()
																									.collect(Collectors.groupingBy(k -> k._2));
													long counter = 0;
													
													int startingSnapshot = snapshot2refs.keySet().stream().mapToInt(snapshot -> snapshot.intValue()).min().getAsInt();
													int lastSnapshot = snapshot2refs.keySet().stream().mapToInt(snapshot -> snapshot.intValue()).max().getAsInt();
		
													int i = startingSnapshot;
													while(i <= lastSnapshot - 1) {
														Set<String> set1 = snapshot2refs.get(i).stream().map(d -> d._1).collect(Collectors.toSet());
														int j = i;
														
														while(j <= lastSnapshot) {
															j = j + 1;
															if(snapshot2refs.get(j) != null) {		
																break;
															}
															
														}

														if(j <= lastSnapshot) {
															Set<String> set2 = snapshot2refs.get(j).stream().map(d -> d._1).collect(Collectors.toSet());
															counter+=countChanges(set1, set2);
														}
														
														i = j;
														
													
														
														
													}
													return new Tuple2<>(x._1, (double)counter);
												})
												  .persist(StorageLevel.MEMORY_ONLY_SER());;
												
		
		if(persistResults) {
			persist(results);
		}
		
		log(INFO, "ended.");

		
		//TODO: Bisogna capire come fare a far tornare un valore nell'intervallo 0-1 e ad utilizzarlo nelle analisi combinate.
		//return collectionMovement.mapToPair(x -> new Tuple2<>(x._1, x._2/maxValue));
		return results;
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 */
	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score <= threshold;
	}
	
	private long countChanges(Set<String> lst1, Set<String> lst2) {
		long disappear = 0;
		long appear = 0;
		
		List<String> changed = new ArrayList<String>();
		for(String s: lst1)
			if(!lst2.contains(s))
				disappear++;
		
		for(String s: lst2)
			if(!lst1.contains(s))
				appear++;
		
		return (disappear <= appear) ? appear-disappear : disappear-appear;
	}
		
}
