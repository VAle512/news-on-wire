package it.uniroma3.pagerank;

import static it.uniroma3.properties.PropertiesReader.PAGE_RANK_GRAPH_GENERATOR_FILE;
import static it.uniroma3.properties.PropertiesReader.PAGE_RANK_ITERATIONS;
import static it.uniroma3.properties.PropertiesReader.MONGODB_DB_NAME;
import static it.uniroma3.properties.PropertiesReader.MONGODB_HOST_ADDRESS;
import static it.uniroma3.properties.PropertiesReader.MONGODB_RANKS_COLLECTION;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

public final class PageRank {
	private static final Pattern SPACES = Pattern.compile("\\s+");
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	final static String mongoDBHost = propsReader.getProperty(MONGODB_HOST_ADDRESS);
	final static String mongoDBName = propsReader.getProperty(MONGODB_DB_NAME);
	private static final File file = new File(propsReader.getProperty(PAGE_RANK_GRAPH_GENERATOR_FILE));
	private static final int numIterations = Integer.parseInt(propsReader.getProperty(PAGE_RANK_ITERATIONS));

	public static void compute(Graph graph) throws Exception {   
		JavaSparkContext ctx = new JavaSparkContext("local[*]", "App");
		JavaPairRDD<String, Iterable<String>> links = ctx.parallelize(Lists.newArrayList(graph.getGraph()), 32)
														 .mapToPair(arch -> new Tuple2<>(arch.getFromNode().getUrl(), arch.getToNode().getUrl()))
														 .distinct()
														 .groupByKey()
														 .cache();
//		System.out.println(file.getAbsolutePath());
//		JavaRDD<String> lines = ctx.textFile(file.getAbsolutePath(), 1);
		//System.out.println(lines.first());
//	    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(line -> {
//				    														String[] parts = SPACES.split(line);
//				    														if(parts.length==2)
//				    															return new Tuple2<String, String>(parts[0], parts[1]);
//				    														else
//				    															return null;
//	    															})
//	    												   .filter(x -> x!=null)
//	    												   .distinct()
//	    												   .groupByKey()
//	    												   .cache();
   
	    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
	    JavaPairRDD<String, Double> ranks = links.mapValues(x -> 1.);

	    // Calculates and updates URL ranks continuously using PageRank algorithm.
	    for (int current = 0; current < numIterations; current++) {
	    	// Calculates URL contributions to the rank of other URLs.
	    	JavaPairRDD<String, Double> contribs = links.join(ranks)
				  									    .values()
				  									    .flatMapToPair(x -> {
				  										  					int urlCount = Iterables.size(x._1);
				  										  					List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
																        	for (String neighbor : x._1) 
																        		results.add(new Tuple2<>(neighbor, x._2 / urlCount));
																        	return results.iterator();
				  									    });

	    	// Re-calculates URL ranks based on neighbor contributions.
	    	ranks = contribs.reduceByKey((x, y) -> x + y)
    		  		  	    .mapValues(x -> 0.15 + x*0.85);
    }

    // Collects all URL ranks and dump them to console.
    List<Tuple2<String, Double>> output = ranks.collect();
    output = output.stream().sorted((x,y) -> (-1) * (x._2.compareTo(y._2))).collect(Collectors.toList());
    
    
    String mongoRanksHost = propsReader.getProperty(MONGODB_RANKS_COLLECTION);
    String mongodbSparkInput = "mongodb://" + mongoDBHost +"/" + mongoDBName + "." + mongoRanksHost;
    Map<String, String> configMap = Maps.newHashMap();
    configMap.put("uri", mongodbSparkInput);
    WriteConfig config = WriteConfig.create(configMap);
    MongoSpark.save(ranks.map(x -> (new Document("url", x._1)).append("rank", x._2)), config);
    
    for (Tuple2<?,?> tuple : output.subList(0, 50)) {
        System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
    }

    ctx.stop();
    ctx.close();
  }
}
