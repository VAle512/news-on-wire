package it.uniroma3.newswire.benchmark.benchmarks;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;

import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.neo4j.driver.internal.util.Iterables;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.persistence.schemas.LinkOccourrences;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

//EXPERIMENTAL: We need more data, at more depth.
//FIXME: Chose an appropriate name for this class.
public class LinkDinamicityFactor extends Benchmark {
	
	/**
	 * Constructor.
	 */
	public LinkDinamicityFactor(String dbName) {
		super(dbName);
	}
	
	@Override
	public JavaPairRDD<String, Double> analyze(boolean persistResults, int untilSnapshot) {
		DAO dao = DAOPool.getInstance().getDAO(this.database);
		/* Erase previous Link Occurrence Dinamicity Data */
		erasePreviousBenchmarkData(persistResults);
															
		/* recalulcate link collections. */
		
		JavaPairRDD<String, Double> link2HypertextualContentDinamicity = (new HyperTextualContentDinamicity(this.database)).analyze(persistResults, untilSnapshot);
		JavaPairRDD<String, String> link2referringPage = loadData().mapToPair(tuple -> new Tuple2<>(tuple.getString(link.name()), tuple.getString(referringPage.name())));
		JavaPairRDD<String, Double> result = link2referringPage.join(link2HypertextualContentDinamicity).map(x -> new Tuple2<>(x._2._1, new Tuple2<>(x._1, x._2._2)))
																	.groupBy(x -> x._1)
																	.mapToPair(x -> new Tuple2<>(x._1,Lists.newArrayList(x._2).stream().mapToDouble(k -> k._2._2).sum()))
																	.join(link2HypertextualContentDinamicity)
																	.mapToPair(x -> {
																		Double score = Math.pow(x._2._2, 3) / x._2._1;
																		score = (score.isInfinite() || score.isNaN()) ? 30. : score;
																		return new Tuple2<>(x._1, score);
																	});
		
		/* Persist results on the DB. 
		 * WARNING: referringPage not persisted but could be useful! 
		 */
		if(persistResults) {
			persist(result);
		}
		
		return result;
	}

}
