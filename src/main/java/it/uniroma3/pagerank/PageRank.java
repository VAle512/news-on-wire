package it.uniroma3.pagerank;

import static it.uniroma3.properties.PropertiesReader.MONGODB_DB_NAME;
import static it.uniroma3.properties.PropertiesReader.MONGODB_HOST_ADDRESS;
import static it.uniroma3.properties.PropertiesReader.PAGE_RANK_ITERATIONS;
import static it.uniroma3.properties.PropertiesReader.MONGODB_RANKS_COLLECTION;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

import it.uniroma3.graphs.Graph;
import it.uniroma3.properties.PropertiesReader;
import scala.Tuple2;

/**
 * This class can calculate the PageRank ranking for every {@link Node} of a {@link Graph}.
 * The purpose is to run it on a single domain to extract informations.
 * @author Luigi D'Onofrio
 *
 */
public final class PageRank {
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final int numIterations = Integer.parseInt(propsReader.getProperty(PAGE_RANK_ITERATIONS));
	private static final Logger logger = Logger.getLogger(PageRank.class);
	final static String mongoDBHost = propsReader.getProperty(MONGODB_HOST_ADDRESS);
	final static String mongoDBName = propsReader.getProperty(MONGODB_DB_NAME);

	/**
	 * @param graph is the {@link Graph} we want to calculate the PageRank on.
	 * @return a {@link Map} containing the {@link Node} and its ranking.
	 * @throws Exception
	 */
	public static Map<String, Float> compute(Graph graph) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext("local[*]", "Spark PageRank");
		
		JavaPairRDD<String, Iterable<String>> links = ctx.parallelize(Lists.newArrayList(graph.getGraphArches()), 32)
														 .mapToPair(arch -> new Tuple2<>(arch.getFromNode().getName(), arch.getToNode().getName()))
														 .distinct()
														 .groupByKey()
														 .cache();
	    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
	    JavaPairRDD<String, Float> ranks = ctx.parallelize(Lists.newArrayList(graph.getGraphNodes()), 16).mapToPair(x -> new Tuple2<>(x.getName(), 1.0f));
	    
	    // Calculates and updates URL ranks continuously using PageRank algorithm.
	    for (int current = 0; current < numIterations; current++) {
	    	
	    	// Calculates URL contributions to the rank of other URLs.
	    	JavaPairRDD<String, Float> contribs = links.join(ranks)
				  									    .values()
				  									    .flatMapToPair(x -> {
				  										  					int urlCount = Iterables.size(x._1);
				  										  					List<Tuple2<String, Float>> results = new ArrayList<Tuple2<String, Float>>();
																        	for (String neighbor : x._1) 
																        		results.add(new Tuple2<>(neighbor, (x._2 / urlCount)));
																        	return results.iterator();
				  									    });
	    	// Re-calculates URL ranks based on neighbor contributions.
	    	// We also add nodes pointed by no-one.
	    	JavaPairRDD<String, Float> noContribs = ctx.parallelize(Lists.newArrayList(graph.getGraphNodes()), 16)
	    												.map(node -> node.getName())
	    												.subtract(contribs.keys())
	    												.mapToPair(x -> new Tuple2<>(x, 0.15f));
	    	
	    	ranks = contribs.reduceByKey((x, y) -> x + y)
    		  		  	    .mapValues(x -> (0.15f + x*0.85f));
	    
	    	ranks = ranks.union(noContribs);
	    	logger.info("Iteration " + current + " completed.");
    }

    // Collects all URL ranks and dump them to console.
    List<Tuple2<String, Float>> output = ranks.collect();
    output = output.stream()
    			   .sorted((x,y) -> (-1) * (x._2.compareTo(y._2)))
    			   .collect(Collectors.toList());
    
    // Write the scores in MongoDB
    String mongoRanksHost = propsReader.getProperty(MONGODB_RANKS_COLLECTION);
    String mongodbSparkInput = "mongodb://" + mongoDBHost +"/" + mongoDBName + "." + mongoRanksHost;
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put("uri", mongodbSparkInput);
    WriteConfig config = WriteConfig.create(configMap);
    //TODO: Remember to drop the DB before this operation
    MongoSpark.save(ranks.map(x -> (new Document("url", x._1)).append("rank", x._2)), config);
    
    Map<String, Float> res = ranks.collectAsMap();
    ctx.stop();
    ctx.close();
    logger.info("Page Rank calculation complete.");
    return res;
  }
}
