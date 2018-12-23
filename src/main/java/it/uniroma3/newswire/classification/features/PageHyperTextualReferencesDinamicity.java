package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.benchmark.utils.LinkCollectionFinder.findCollections;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static org.apache.log4j.Level.INFO;

import java.util.List;
import java.util.Map;
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
public class PageHyperTextualReferencesDinamicity extends Feature{
	
	/**
	 * Constructor.
	 * @param dbName is the database of the website we are executing the benchmark for.
	 */
	public PageHyperTextualReferencesDinamicity(String dbName) {
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
		if(persistResults)
			erasePreviousBenchmarkData(persistResults);
		
		/* recalulcate link collections. */
		JavaPairRDD<Long, String> collections =  getCollections(getAssociatedDAO(), untilSnapshot);
		
		//FIXME: Debug purposes swapped relative and referringPage
		JavaPairRDD<Long, Tuple4<String, String, String, Integer>> data = loadData().mapToPair(row -> new Tuple2<>(row.getLong(id.name()),
																							  						 new Tuple4<>(
																							  								 row.getString(link.name()),
																							  								 row.getString(referringPage.name()),
																							  								 row.getString(xpath.name()),
																							  								 row.getInteger(snapshot.name()))
																							  							)
																									);
		
		Map<String, Long> link2Counterino = data.map(x -> x._2)
														.groupBy(x -> x._1())
														.mapToPair(x -> new Tuple2<>(x._1, Lists.newArrayList(x._2).stream().count()))
														.collectAsMap();

		
		if(untilSnapshot > 0)
			data = data.filter(x -> x._2._4() <= untilSnapshot);
		
		JavaPairRDD<Tuple3<String, String, String>, Tuple2<String, Integer>> join = data.join(collections).mapToPair(x -> new Tuple2<>(
																														  		new Tuple3<>(
																														  				x._2._1._1(), 
																														  				x._2._1._2(), 
																														  				x._2._2),
																														  		new Tuple2<>(
																														  				x._2._1._3(),
																														  				x._2._1._4())));
		
		
		/* Here we group by (link, referring page) to isolate link occurrences 
		 * of the same page but picked in different snapshots.
		 */
		JavaPairRDD<String, Double> collectionMovement = join
									  						/* Some XPath could be null, better to remove them. */
									  						.filter(row -> row._1._3()!=null && row._2._1()!=null)
									  						.groupBy(pair -> pair._1)   
									  						.map(group -> { 
									  							/* Let's count how many "XPath's changes" there are for a link across snaphshot in the same link collection. */
									  							Set<Tuple2<String, Integer>> xpath2Snapshot = Iterables.asList(group._2).stream().map(x -> x._2).collect(Collectors.toSet());			  
									  							Set<List<Tuple2<String, Integer>>> allPossiblesCouples = Sets.cartesianProduct(xpath2Snapshot, xpath2Snapshot);	
									  							
									  							
									  							/* We create this collection which has in it all those tuples which have a sibling avross a snapshot.
									  							 * This is useful when we have two link occurrences whihc point to the same page and we cannot distinguish between them.
									  							 */
									  							final LinkedBlockingQueue<Tuple2<Integer, Integer>> misleadingEqualities = new LinkedBlockingQueue<>(
									  																								  allPossiblesCouples.stream()
									  																			 					  						.map(x -> new Tuple2<>(x.get(0), x.get(1)))
									  																			 					  						.filter(tuple -> tuple._1._2 < tuple._2._2)
									  																			 					  						.collect(Collectors.groupingBy(x -> x._1._2))
									  																			 					  						.values()
									  																			 					  						.stream()
									  																			 					  							.map(tuples -> {
									  																			 					  								int min = tuples.stream().mapToInt(x -> x._2._2).min().getAsInt();
									  																			 					  								return tuples.stream().filter(t -> t._2._2 == min);
									  																			 					  							})
									  																			 					  							.flatMap(x -> x)
									  																			 					  							.filter(tuple -> tuple._1._1.equals(tuple._2._1))
									  																			 					  							.map(tuple -> new Tuple2<>(tuple._1._2, tuple._2._2))
									  																			 					  							.collect(Collectors.toList()));
									  				
									  							List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> differences = allPossiblesCouples.stream()
							  										       																				.map(x -> new Tuple2<>(x.get(0), x.get(1)))
							  										       																				.filter(tuple -> tuple._1._2 < tuple._2._2)
							  										       																				.collect(Collectors.groupingBy(x -> x._1))
							  										       																				.values()
							  										       																				.stream()
							  										       																					.map(tuples -> {
							  										       																						int min = tuples.stream().mapToInt(x -> x._2._2).min().getAsInt();
							  										       																						return tuples.stream().filter(t -> t._2._2 == min).collect(Collectors.toList());
							  										       																					})
							  										       																					.flatMap(x -> x.stream()) 
							  										       																					.filter(x -> !x._1._1.equals(x._2._1))
							  										       																					.distinct()
							  										       																					.collect(Collectors.toList());
										  								/* Let's remove all the misleading entries to make the result be more truthful. */
										  								for(Tuple2<Integer, Integer> err: misleadingEqualities) {
										  									boolean contained = differences.stream()
										  														 .map(x -> new Tuple2<>(x._1._2, x._2._2))
										  														 .anyMatch(x -> x.equals(err));
										  									if(contained) {
										  										Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> toRemove = differences.stream()
										  																											 .filter(x -> (new Tuple2<>(x._1._2, x._2._2)).equals(err))
										  																											 .findFirst()
										  																											 .get();
										  										differences.remove(toRemove);																	
										  									}
										  								}
										  								
										  						long count = differences.stream().filter(x -> !x._1._1.equals(x._2._1)).count();
			      
									  							return new Tuple2<>(group._1, count);
									  						})
									  						.groupBy(x -> x._1._1())
									  						//TODO: normalize on num of occurrences
									  						.map(x -> {
//									  							double count = (link2Counterino.get(x._1) != null) ? link2Counterino.get(x._1): 1;
									  							long differences = Iterables.asList(x._2).stream().mapToLong(y -> y._2).sum();
									  							return new Tuple2<>(x._1, differences);
									  						})
									  						.mapToPair(tuple -> new Tuple2<>(tuple._1, new Double(tuple._2)));
		//DEPRECATED: For normalization
		double maxValue = collectionMovement.mapToDouble(x -> x._2).max();


		if(persistResults) {
			persist(collectionMovement.mapToPair(x -> new Tuple2<>(x._1, x._2/maxValue)));
		}
		
		log(INFO, "ended.");

		
		//TODO: Bisogna capire come fare a far tornare un valore nell'intervallo 0-1 e ad utilizzarlo nelle analisi combinate.
		//return collectionMovement.mapToPair(x -> new Tuple2<>(x._1, x._2/maxValue));
		return collectionMovement.mapToPair(x -> new Tuple2<>(x._1, x._2/maxValue));
	}
	
	/**
	 * Wrapper method that resturns all the correspondences between id and relative collection.
	 * @param dao is the DAO we are working on.
	 * @param snapshot is tu snapshot we are calculating the collections for.
	 * @return an RDD made of couples (id, collection).
	 */
	private JavaPairRDD<Long, String> getCollections(DAO dao, int snapshot) {
		/* Take the property */
		boolean persistLinkCollections = Boolean.parseBoolean(propsReader.getProperty(PropertiesReader.BENCHMARK_PERSIST_LINK_COLLECTIONS));
		
		
		if(persistLinkCollections)
			dao.createLinkCollectionsTable();
		
		List<Tuple2<String, Set<Long>>> collection2ids = findCollections(dao.getXPathsUntil(snapshot));
		
		JavaPairRDD<Long, String> id2collectionRDD =  SparkLoader.getInstance().getContext()
																			 .parallelize(collection2ids)
																			 .map(x -> x._2.stream().map(id -> new Tuple2<>(x._1, id)))
																			 .flatMap(x -> x.iterator())
																			 .mapToPair(x -> new Tuple2<>(x._2, x._1));
		if(persistLinkCollections)
			dao.updateCollections(collection2ids);
		
		return id2collectionRDD;
	}

	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.benchmark.benchmarks.Benchmark#isThresholded(java.lang.Double, java.lang.Double)
	 */
	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score <= threshold;
	}
	
	private double sigmoidOf(Long n) {
		return 1 / (1 + Math.exp(-n));
	}
		
}
