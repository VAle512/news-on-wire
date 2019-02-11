package it.uniroma3.newswire.crawling;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.spark_project.guava.collect.Lists;

import com.google.common.io.Files;

import it.uniroma3.newswire.classification.features.DataLoader;
import it.uniroma3.newswire.classification.features.Feature;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;

public class FeatureExtractionDriver {
	private String root;
	private List<Feature> features;
	
	public FeatureExtractionDriver(String websiteRootName, List<Feature> features) {
		this.root = websiteRootName;
		this.features = features;
	}
	
	public void extractFeatures(int snapshot, boolean persist) {
		String dbName = URLUtils.getDatabaseNameOf(this.root);
		File csvFile = new File(System.getenv(EnvironmentVariables.datasets) + "/simulation/" + dbName + "/" + dbName + "_" + snapshot + "_training.csv");
		System.out.println(csvFile.getAbsolutePath());
		System.out.println(features.stream().map(x -> x.getClass().getSimpleName()).collect(Collectors.toList()));
		Map<String, List<Double>> url2Scores = new HashMap<>();

		for(Feature feature: features) {
			feature.calculate(persist, snapshot).collect().stream().forEach(x -> {
				String key = x._1;
				Double score = x._2;
				if(url2Scores.containsKey(key)) {
					url2Scores.get(key).add(score);
				} else {
					url2Scores.put(key, Lists.newArrayList(score));
				}			
			});
		}
		
		int currentnumberOfFeatures = url2Scores.values().stream().mapToInt(x -> x.size()).max().getAsInt();
		
		/*
		 * Ottieni i golden samples dal DB.
		 */
		Map<String, Integer> goldenClasses = DataLoader.getInstance().loadGoldenData(dbName).collectAsMap();
		if(goldenClasses.size() == 0 || goldenClasses == null)
			System.out.println("Impossibile trovare i golden. Fornirli e salvarli sul DB.");
		
		/* Appendiamo la classe */
		//TODO: Spostare in altra classe per renedere modulare.
		goldenClasses.forEach((k, v) -> {
			if(url2Scores.containsKey(k))
				url2Scores.get(k).add((double) v);
		});
		
		/*
		 * Cancelliamo il contenuto del file precedente (se esiste).
		 */
		try {
			System.out.println(csvFile.getParentFile().mkdirs());
			System.out.println(csvFile.createNewFile());
			Files.write("", csvFile, StandardCharsets.UTF_8);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		url2Scores.entrySet().stream()
								.filter(entry -> entry.getValue().size() < currentnumberOfFeatures)
								.filter(entry -> !entry.getValue().contains(-1.))
								.forEach(entry -> {
									try {
										StringBuilder line = new StringBuilder(entry.getKey());
										entry.getValue().forEach(score -> line.append("," + score));
										line.append("\n");
										Files.append(line, csvFile, StandardCharsets.UTF_8);
									} catch (IOException e) {
										e.printStackTrace();
									}
								});
		
		DataLoader.getInstance().incrementSnapshotTo(snapshot);
	}
	
	public void simulateExtractFeatures(List<String> urls, int snapshot, boolean persist) throws Exception {
		String dbName = URLUtils.getDatabaseNameOf(this.root);
		File csvFile = new File(System.getenv(EnvironmentVariables.datasets) + "/simulation/" + dbName + "/" + dbName + "_" + snapshot + "_training.csv");
		
		Map<String, List<Double>> url2Scores = new HashMap<>();

		for(Feature feature: features) {
			feature.calculate(persist, snapshot).foreach(x -> {
				String key = x._1;
				Double score = x._2;
				/* Filtra su quelli effettivamente crawlati. */
				if(!urls.contains(key))
					return;
				
				if(url2Scores.containsKey(key)) {
					url2Scores.get(key).add(score);
				} else {
					url2Scores.put(key, Lists.newArrayList(score));
				}			
			});
		}
		
		
		int currentnumberOfFeatures = url2Scores.values().stream().mapToInt(x -> x.size()).max().getAsInt();
		
		/*
		 * Ottieni i golden samples dal DB.
		 */
		Map<String, Integer> goldenClasses = DataLoader.getInstance().loadGoldenData(dbName).collectAsMap();
		if(goldenClasses.size() == 0 || goldenClasses == null)
			throw new Exception("Impossibile trovare i golden. Fornirli e salvarli sul DB.");
		
		/* Appendiamo la classe */
		//TODO: Spostare in altra classe per renedere modulare.
		goldenClasses.forEach((k, v) -> {
			if(url2Scores.containsKey(k))
				url2Scores.get(k).add((double) v);
		});
		
		/*
		 * Cancelliamo il contenuto del file precedente (se esiste).
		 */
		Files.write("", csvFile, StandardCharsets.UTF_8);
		
		url2Scores.entrySet().stream()
								.filter(entry -> entry.getValue().size() < currentnumberOfFeatures)
								.filter(entry -> !entry.getValue().contains(-1.))
								.forEach(entry -> {
									try {
										StringBuilder line = new StringBuilder(entry.getKey());
										entry.getValue().forEach(score -> line.append("," + score));
										line.append("\n");
										Files.append(line, csvFile, StandardCharsets.UTF_8);
									} catch (IOException e) {
										e.printStackTrace();
									}
								});
		
		DataLoader.getInstance().incrementSnapshotTo(snapshot);
	}

}
