package it.uniroma3.newswire.benchmark;

import static it.uniroma3.newswire.utils.EnvironmentVariables.goldens;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.spark_project.guava.collect.Lists;

import scala.Tuple2;

public class BenchmarkClustering implements Serializable {
	private static final long serialVersionUID = -582250295537870328L;
	private List<String> cachedGoldenData;

	public BenchmarkClustering(String website) throws IOException {
		this.cachedGoldenData = Files.readAllLines(Paths.get(System.getenv(goldens) + File.separator + website + ".csv"), StandardCharsets.UTF_8);
	}
	
	public void printResults(JavaPairRDD<Integer, String> data) {
		data.groupBy(x -> x._1)
							   .mapToPair(x -> {
			List<String> clusterPositives = Lists.newArrayList(x._2).stream().map(k-> k._2).collect(Collectors.toList());
			Double precision = precision(clusterPositives);
			Double recall = recall(clusterPositives);		
			return new Tuple2<>(x._1, new Tuple2<>(precision, recall));
		}).foreach(x -> System.out.println(x));
							  
	}
	/**
	 * @param positives is the data we want to calculate the Recall of.
	 * @return the Recall value. 
	 */
	public Double recall(List<String> positives){
		long relevantFound = 0;	
		
		for(String url: this.cachedGoldenData)
			if(positives.contains(url))
				++relevantFound;
		
		return (double)relevantFound/(double)cachedGoldenData.size();
	}
	
	/**
	 * @param positives is the data we want to calculate the Precision of.
	 * @return the Precision value.
	 */
	public Double precision(List<String> positives){
		if(positives.size()==0)
			return 0.;
	
		long relevantFound = 0;
		
		for(String url: positives)
			if(cachedGoldenData.contains(url))
				++relevantFound;

		return (double)relevantFound/(double)positives.size();
	}
}
