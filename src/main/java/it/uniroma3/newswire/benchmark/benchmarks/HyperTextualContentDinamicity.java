package it.uniroma3.newswire.benchmark.benchmarks;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.*;
import static it.uniroma3.newswire.properties.PropertiesReader.BENCHMARK_REDO_ALL;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.neo4j.driver.internal.util.Iterables;

import com.google.common.collect.Sets;

import it.uniroma3.newswire.benchmark.utils.LinkCollectionFinder;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/**
 * This class is intended to quantify the motion of a link inside a specific page and link collection.
 * in terms of how many xpath changes it counts across all the pages that reference that link.
 * @author Luigi D'Onofrio
 *
 */
public class HyperTextualContentDinamicity extends Benchmark{
	private static final long serialVersionUID = 4965813350830416479L;

	/**
	 * Constructor.
	 */
	public HyperTextualContentDinamicity(String dbName) {
		this.resultsTable = this.getClass().getSimpleName();
		this.database = dbName;
		init();
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.analysis.Analysis#analyze(boolean)
	 */
	@SuppressWarnings("unchecked")
	public JavaPairRDD<String, Double> analyze(boolean persist, int untilSnapshot) {
		/* Erase previous Link Occurrence Dinamicity Data */
		if(persist) {
			DAO dao = DAOPool.getInstance().getDAO(this.database);
			if(dao.checkTableExists(this.resultsTable))
				dao.cleanTable(this.resultsTable);
			else
				dao.createAnalysisTable(this.resultsTable);
			
			//TODO: capire se questo crea problemi con il join
			boolean benchmarkRedoAll = Boolean.parseBoolean(propsReader.getProperty(BENCHMARK_REDO_ALL));
			if(benchmarkRedoAll) {
				if(dao.checkTableExists("LinkCollections")) {
					long linkOccurrencesCount = dao.count("LinkOccurrences");
					long collectionsCount = dao.count("LinkCollections");
					
					double proportion = (double) collectionsCount / (double) linkOccurrencesCount;
					
					if(proportion < 0.9)
						processCollections(dao);
				} else {
					processCollections(dao);
				}
				
					
			} else {
				processCollections(dao);
			}
		}
		
	
		/* load up the link collections */
		JavaPairRDD<Integer, String> collections =  loadLinkCollections();
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
									  						//TODO: lol
									  						.groupBy(pair -> pair._1)   
									  						.map(group -> { 
									  							/* Let's count how many "XPath's changes" there are for a link across snaphshot. */
									  							Set<Tuple2<String, Integer>> xpath2snapshot = Iterables.asList(group._2).stream().map(x -> x._2).collect(Collectors.toSet());			  
									  							Set<List<Tuple2<String, Integer>>> couples = 	Sets.cartesianProduct(xpath2snapshot, xpath2snapshot);	
										  
									  							List<Tuple2<Integer, Integer>> erroneus = couples.stream()
									  																			 .map(x -> new Tuple2<>(x.get(0), x.get(1)))
									  																			 .filter(tuple -> tuple._1._2 < tuple._2._2)
									  																			 .filter(tuple -> tuple._1._1.equals(tuple._2._1))
									  																			 .map(tuple -> new Tuple2<>(tuple._1._2, tuple._2._2))
									  																			 .collect(Collectors.toList());
										  
									  							int count = couples.stream()
									  										       .map(x -> new Tuple2<>(x.get(0), x.get(1)))
									  										       .filter(tuple -> (tuple._1._2 < tuple._2._2)&&(!erroneus.contains(new Tuple2<>(tuple._1._2, tuple._2._2))))
									  										       .mapToInt(x -> {
									  										    	   int val = (!x._1._1.equals(x._2._1)) ? 1 : 0;
									  										    	   return val;
									  										       })
									  										       .sum();
	
									  							return new Tuple2<>(group._1, count);
									  						})
									  						.groupBy(x -> x._1._1())
									  						.map(x -> new Tuple2<>(x._1,Iterables.asList(x._2).stream().mapToInt(y -> y._2).sum()))
									  						.mapToPair(tuple -> new Tuple2<>(tuple._1, new Double(tuple._2.intValue())));
		
		/* Persist results on the DB. 
		 * WARNING: referringPage not persisted but could be useful! 
		 */
		if(persist) {
			persist(collectionMovement, resultsTable);
		}

		return collectionMovement;
	}
	
	private JavaPairRDD<Integer, String> loadLinkCollections() {
		String url = DAO.DB_URL.replace(DB_NAME_PLACEHOLDER, this.database);
		
		return sqlContext.read()
			  	   		 .format("jdbc")
			  	   		 .option("url", url)
			  	   		 .option("driver", DAO.JDBC_DRIVER)
			  	   		 .option("dbtable", "LinkCollections")
			  	   		 .option("user", DAO.USER)
			  	   		 .option("password", DAO.PASS)
			  	   		 .load()
			  	   		 .toJavaRDD()
			  	   		 .mapToPair(row -> new Tuple2<>(row.getInt(0), row.getString(1)));

	}


	public String getCanonicalBenchmarkName() {
		return HyperTextualContentDinamicity.class.getSimpleName()+"Benchmark";
	}

	public String getBenchmarkSimpleName() {
		return HyperTextualContentDinamicity.class.getSimpleName();
	}
	
	private void processCollections(DAO dao) {
		dao.createLinkCollectionsTable();
		List<Tuple2<String,  Set<Integer>>> collection2ids = LinkCollectionFinder.findCollections(dao.getXPaths());
		dao.updateCollections(collection2ids);
	}
}
