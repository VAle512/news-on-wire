package it.uniroma3.newswire.classification;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envmodels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.DistanceMeasure;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import com.google.common.collect.Lists;

import it.uniroma3.newswire.benchmark.BenchmarkClustering;
import it.uniroma3.newswire.classification.features.PageHyperTextualContentDinamicity;
import it.uniroma3.newswire.classification.features.PageHyperTextualReferencesDinamicity;
import it.uniroma3.newswire.classification.features.Stability;
import scala.Tuple2;
import scala.Tuple3;

//DEPRECATED
//TODO: Commentare questa classe.
/**
 * Si occupa di clusterizzare i dati in base a delle features. E' un approccio non supervisionato che potrebbe andar bene per il nostro lavoro.
 * Al momento non viene utilizzato ma potrebbe esserlo in futuro.
 * ATTENZIONE: I dati hanno bsiogno di subire una normalizzazione per essere utilizzati con questo algoritmo.
 * @author luigi
 *
 */
@SuppressWarnings("unused")
public class Clusterer {
	private static String cachedModelsPath = System.getenv(envmodels) + "/";
	
	public static void calculate(String dbName, int snapshot) {
		/* Benchmarks */
		Stability stability = new Stability(dbName);
		PageHyperTextualReferencesDinamicity hrd = new PageHyperTextualReferencesDinamicity(dbName);
		PageHyperTextualContentDinamicity hcd= new PageHyperTextualContentDinamicity(dbName);
		
		JavaPairRDD<String, Double> stabilityResults = stability.calculate(true, snapshot).cache();
		
		JavaPairRDD<String, Double> hrdResults = hrd.calculate(true, snapshot).cache();
		
		JavaPairRDD<String, Double> hcdResults = hcd.calculate(true, snapshot).cache();
		
		JavaPairRDD<String, Tuple3<Double, Double, Double>> joinResults = stabilityResults.join(hrdResults).join(hcdResults)
																						.mapToPair(x -> new Tuple2<>(x._1, 
																													new Tuple3<>(x._2._1._1, 
																																 x._2._1._2, 
																																 x._2._2)))
																						.cache();

		JavaPairRDD<String, Vector> link2Features = joinResults.mapToPair(s -> {
														  int length = 3;
														  double[] values = new double[length];
														  
														  values[0] = s._2._1(); //stability
														  values[1] = s._2._2(); // dinamicity ref
														  values[2] = s._2._3(); // dinamicity content
														  
														  return new Tuple2<>(s._1, Vectors.dense(values));
														});
				
		
		JavaRDD<Vector> points = link2Features.map(x -> x._2);
		link2Features.cache();
	
		// Cluster the data into three classes using KMeans (Section, Article, Other)
		int numClusters = 3;
		int numIterations = 20;
		List<Vector> centers = new ArrayList<>();
		double[] sectionCentroid = {1.0, 0.0};
		double[] articleCentroid = {0.0, 0.1};
		centers.add(Vectors.dense(sectionCentroid));
		centers.add(Vectors.dense(articleCentroid));
		
		
		KMeansModel startingModel = new KMeansModel(centers);
		KMeansModel clusters = (new KMeans()).setInitializationSteps(10).setDistanceMeasure(DistanceMeasure.EUCLIDEAN()).setMaxIterations(20).run(points.rdd());
		
//		Map<Integer, List<Tuple2<Double, Double>>> cluster2Points = link2Features.mapToPair(x -> {
//			return new Tuple2<>(clusters.predict(x._2), new Tuple2<>(x._2.toArray()[0], x._2.toArray()[1]));
//		}).groupBy(x -> x._1)
//		  .mapToPair(group -> new Tuple2<>(group._1, Lists.newArrayList(group._2).stream().map(k -> k._2).collect(Collectors.toList())))
//		  .collectAsMap();
		
		
		
//		Visualizer visual = new Visualizer("kmeans", cluster2Points);
//		visual.visualize();
		
		System.out.println("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
		  System.out.println(center.toJson());
		}
		
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
		
		//cluster2Link.groupBy(x -> x._1).map(x -> new Tuple2<>(x._1, (Lists.newArrayList(x._2).size() <= 100) ? Lists.newArrayList(x._2) : Lists.newArrayList(x._2).subList(0, 100))).foreach(x -> System.out.println(x));
		
		// Save and load model
//		SparkContext sc = SparkLoader.getInstance().getContext().sc();
//		clusters.save(sc, cachedModelsPath + "KMeansModel_" + dbName);
	}
}
