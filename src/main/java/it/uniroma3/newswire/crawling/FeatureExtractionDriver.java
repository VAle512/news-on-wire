package it.uniroma3.newswire.crawling;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import com.google.common.io.Files;

import it.uniroma3.newswire.classification.features.DataLoader;
import it.uniroma3.newswire.classification.features.Feature;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;

/**
 * Questo è il driver che guida la fase di Feature-Extraction.
 * @author Luigi D'onofrio
 *
 */
public class FeatureExtractionDriver {
	private static Logger logger = Logger.getLogger(FeatureExtractionDriver.class);
	private String root;
	private List<Feature> features;
	
	public FeatureExtractionDriver(String websiteRoot, List<Feature> features) {
		this.root = websiteRoot;
		this.features = features;
	}
	
	public void extractFeatures(int snapshot, boolean persist) {
		String dbName = URLUtils.getDatabaseNameOf(this.root);
		File csvFile = new File(System.getenv(EnvironmentVariables.datasets) + "/simulation/" + dbName + "/" + dbName + "_" + snapshot + "_training.csv");
		
		/* Se il file esiste già non è necessario effettuare alcuna operazione di estrazione */
		if(csvFile.exists()) {
			logger.info("CSV already present! Skipping.");
			return;
		}
		
		/* Creiamo la mappa con URL e scores delle varie features. */
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
		
		/*
		 * Proviamo a creare il file, se esiste sscriviamo nulla al suo interno.
		 */
		try {
			System.out.println(csvFile.getParentFile().mkdirs());
			System.out.println(csvFile.createNewFile());
			Files.write("", csvFile, StandardCharsets.UTF_8);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		/* Serve perchè potrebbero esserci delle features non calcolate correttamente o non essere proprio calcolate */
		int currentnumberOfFeatures = url2Scores.values().stream().mapToInt(x -> x.size()).max().getAsInt();
		
		/* Annotiamo le osservazioni. */
		labelObservations(url2Scores);
		
		/* Andiamo a scrivere le osservazioni sul file .csv */
		url2Scores.entrySet().stream()
								.filter(entry -> entry.getValue().size() == currentnumberOfFeatures + 1) // Filtriamo solo per URL correttamente estratti.
								.filter(entry -> !entry.getValue().contains(-1.)) // Filtriamo solo per fatures calcolate correttamente.
								.forEach(entry -> {
									try {
										/* Costruiamo la stringa da scrivere nel file .csv */
										StringBuilder line = new StringBuilder(entry.getKey());
										entry.getValue().forEach(score -> line.append("," + score));
										line.append("\n");
										Files.append(line, csvFile, StandardCharsets.UTF_8);
									} catch (IOException e) {
										e.printStackTrace();
									}
								});
		
	}
	
	/**
	 * Questo metodo serve ad aggiungere la label alle varie osservazioni.
	 * @param url2Scores
	 */
	private void labelObservations(Map<String, List<Double>> url2Scores) {
		String dbName = URLUtils.getDatabaseNameOf(this.root);
		
		/*
		 * Ottieni i golden samples dal DB.
		 */
		Map<String, Integer> goldenClasses = DataLoader.getInstance().loadGoldenData(dbName).collectAsMap();
		if(goldenClasses.size() == 0 || goldenClasses == null)
			System.out.println("Impossibile trovare i golden. Fornirli e salvarli sul DB.");
		
		/* Appendiamo la classe */
		goldenClasses.forEach((k, v) -> {
			if(url2Scores.containsKey(k)) 
				url2Scores.get(k).add((double) v);
		});
	}
	
}
