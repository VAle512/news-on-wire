package it.uniroma3.newswire.classification;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envmodels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.DistanceMeasure;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.google.common.collect.Lists;

import it.uniroma3.newswire.benchmark.BenchmarkClustering;
import it.uniroma3.newswire.classification.features.PageHyperTextualReferencesDinamicity;
import it.uniroma3.newswire.classification.features.Stability;
import it.uniroma3.newswire.spark.SparkLoader;
import scala.Tuple2;

public class ThreeMeans {
	private static String cachedModelsPath = System.getenv(envmodels) + "/";
	
	public static void calculate(String dbName, int snapshot) {
		/* Benchmarks */
		Stability stability = new Stability(dbName);
		PageHyperTextualReferencesDinamicity hcd = new PageHyperTextualReferencesDinamicity(dbName);
			
		JavaPairRDD<String, Double> stabilityResults = stability.calculate(true, snapshot).cache();
		
		JavaPairRDD<String, Double> hcdResults = hcd.calculate(true, snapshot).cache();
		
		JavaPairRDD<String, Tuple2<Double, Double>> joinResults = stabilityResults.join(hcdResults).cache();

		JavaPairRDD<String, Vector> link2Features = joinResults.mapToPair(s -> {
														  int length = 2;
														  double[] values = new double[length];
														  
														  values[1] = s._2._1;
														  values[0] = s._2._2;
														  
														  return new Tuple2<>(s._1, Vectors.dense(values));
														});
		JavaRDD<Vector> points = link2Features.map(x -> x._2);
		link2Features.cache();
	
		// Cluster the data into three classes using KMeans (Section, Article, Other)
		int numClusters = 3;
		int numIterations = 20;
		List<Vector> centers = new ArrayList<>();
		double[] sectionCentroid = {1.0, 0.0};
		double[] articleCentroid = {0.0, 0.3};
		centers.add(Vectors.dense(sectionCentroid));
		centers.add(Vectors.dense(articleCentroid));
		
		
		KMeansModel startingModel = new KMeansModel(centers);
		KMeansModel clusters = (new KMeans()).setInitializationSteps(10).setK(2).setDistanceMeasure(DistanceMeasure.EUCLIDEAN()).setMaxIterations(20).run(points.rdd());
		
		JavaPairRDD<Integer, String> cluster2Link = link2Features.mapToPair(x -> {
			return new Tuple2<>(clusters.predict(x._2), x._1);
		});
		
		try {
			BenchmarkClustering benchmark = new BenchmarkClustering(dbName);
			benchmark.printResults(cluster2Link);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		cluster2Link.groupBy(x -> x._1).map(x -> new Tuple2<>(x._1, (Lists.newArrayList(x._2).size() <= 100) ? Lists.newArrayList(x._2) : Lists.newArrayList(x._2).subList(0, 100))).foreach(x -> System.out.println(x));
		
		System.out.println("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
		  System.out.println(" " + center);
		}
		
		// Save and load model
		SparkContext sc = SparkLoader.getInstance().getContext().sc();
		clusters.save(sc, cachedModelsPath + "KMeansModel_" + dbName);
	}
}
