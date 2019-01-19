package it.uniroma3.newswire.classification;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.io.Files;

import it.uniroma3.newswire.classification.features.DataLoader;
import it.uniroma3.newswire.classification.features.PageHyperTextualContentDinamicity;
import it.uniroma3.newswire.classification.features.PageHyperTextualReferencesDinamicity;
import it.uniroma3.newswire.classification.features.PageHypertextualReferenceTrippingFactor;
import it.uniroma3.newswire.classification.features.Stability;
import it.uniroma3.newswire.classification.features.TextToBodyRatio;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;
import scala.Tuple2;
import scala.Tuple6;

/**
 * Questa classe si occupa di generare e costruire un dataset per un addestramento.
 * Va a calcolare le features generando dei .csv con i dati all'interno, ognuno dei quali apparterrà 
 * ad una classe fornita tramite un golden sample memorizzato sul DB.
 * @author luigi
 *
 */
public class TrainingDataGenerator {
	public void generateTrainingSet(String website, int snapshot) throws Exception {
		String dbName = URLUtils.getDatabaseNameOf(website);
		
		/*
		 * Calcola una serie di features da usare come training set.
		 */
		Stability stability = new Stability(dbName);
		PageHyperTextualReferencesDinamicity hrd = new PageHyperTextualReferencesDinamicity(dbName);
		PageHyperTextualContentDinamicity hcd = new PageHyperTextualContentDinamicity(dbName);
		PageHypertextualReferenceTrippingFactor trip = new PageHypertextualReferenceTrippingFactor(dbName);
		TextToBodyRatio ttbr = new TextToBodyRatio(dbName, website);
		
		File csvFile = new File(System.getenv(EnvironmentVariables.datasets) + "/" + dbName + "_" + snapshot + "_training.csv");
		
		JavaPairRDD<String, Double> stabilityRDD = stability.calculate(false, snapshot);
		JavaPairRDD<String, Double> dinamicityRDD = hrd.calculate(true, snapshot); // Questa feature viene memorizzata sul DB poihcè serve per il calcolo di altre features.
		JavaPairRDD<String, Double> contentRDD = hcd.calculate(false, snapshot);
		JavaPairRDD<String, Double> trustnessRDD = trip.calculate(false, snapshot);
		JavaPairRDD<String, Double> textRDD = ttbr.calculate(false, snapshot);
		
		/*
		 * Ottieni i golden samples dal DB.
		 */
		JavaPairRDD<String, Integer> goldenClasses = DataLoader.getInstance().loadGoldenData(dbName);
		if(goldenClasses.count() == 0 || goldenClasses == null)
			throw new Exception("Impossibile trovare i golden. Fornirli e salvarli sul DB.");
		
		/*
		 * Cancelliamo il contenuto del file precedente (se esiste).
		 */
		Files.write("", csvFile, StandardCharsets.UTF_8);
		
		/*
		 * Andiamo a mettere in fila, separati da virgole, tutti i valori delle features.
		 * In cima abbiamo il link, utile per controllare, in coda abbiamo la classe di appartenenza.
		 */
		stabilityRDD.join(dinamicityRDD).join(contentRDD).join(trustnessRDD).join(textRDD).join(goldenClasses)
					.mapToPair(x -> {
						double stabilityValue = x._2._1._1._1._1._1;
						double dinamicityValue = x._2._1._1._1._1._2;
						double contentDinamicityValue = x._2._1._1._1._2;
						double referenceTrippingFactorValue = x._2._1._1._2;
						double textToBodyRatioValue = x._2._1._2;
						
						int cls = x._2._2;
						
						return new Tuple2<>(x._1, new Tuple6<>(stabilityValue, 
																dinamicityValue, 
																contentDinamicityValue, 
																referenceTrippingFactorValue, 
																textToBodyRatioValue, 
																cls));
					})
					.map(x -> x._1 + "," + 
								x._2._1() + "," + 
								x._2._2() + "," + 
								x._2._3() + "," + 
								x._2._4() + "," + 
								x._2._5() + "," + 
								x._2._6() + "\n")
					.filter(x -> !x.contains("-1.0")) //Rimuoviamo i dati non buoni.
					.collect()
					.forEach(x -> {
						try {
							Files.append(x, csvFile, StandardCharsets.UTF_8);
						} catch (IOException e) {
							e.printStackTrace();
						}
					});
		/*
		 * Cancelliamo i dati memorizzati sul DB per quanto riguarda le features che servivano anche in passi intermedi.
		 */
		DAOPool.getInstance().getDAO("nytimes_com").cleanTable(PageHyperTextualReferencesDinamicity.class.getSimpleName());
		
		
	}
	

}
