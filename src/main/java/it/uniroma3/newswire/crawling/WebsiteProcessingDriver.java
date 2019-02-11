package it.uniroma3.newswire.crawling;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.parquet.Files;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import it.uniroma3.newswire.classification.WebsiteClass;
import it.uniroma3.newswire.properties.WebsiteConfiguration;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;

public class WebsiteProcessingDriver extends Thread{
	private static Logger logger = Logger.getLogger(WebsiteProcessingDriver.class);
	private WebsiteConfiguration conf;
	private CrawlingDriver crawlingDriver;
	private FeatureExtractionDriver featureExtractionDriver;
	private ClassificationDriver classificationDriver;
	private Map<String, WebsiteClass> knowledgeBase;
	
	private boolean terminationCondition;
	private int snapshotCounter;
	
	public WebsiteProcessingDriver(CrawlingDriver crawlingDriver, 
									FeatureExtractionDriver feDriver, 
									ClassificationDriver classificationDriver,
									WebsiteConfiguration conf) {
		this.crawlingDriver = crawlingDriver;
		this.featureExtractionDriver = feDriver;
		this.classificationDriver = classificationDriver;
		this.terminationCondition = true;
		this.snapshotCounter = 2;
		this.conf = conf;
	}
	
	public WebsiteProcessingDriver(CrawlingDriver crawlingDriver, 
									FeatureExtractionDriver feDriver, 
									ClassificationDriver classificationDriver,
									WebsiteConfiguration conf,
									int processUntilSnapshot) {
		this(crawlingDriver, feDriver, classificationDriver, conf);
		
		this.terminationCondition = (this.snapshotCounter > processUntilSnapshot);
		
	}
	
	public void run () {
		try {
			simulateRun();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	private void realRun() {
		/* While termination-condition */
		while(!this.terminationCondition) { 
			/* Crawling */
			this.crawlingDriver.startCrawling();
			
			/* Quando termina il crawling iniziare la fase di feature-extraction */
			try {
				this.featureExtractionDriver.extractFeatures(this.snapshotCounter, false);
			} catch (Exception e) {
				logger.error(this.conf.getWebsite() + " -- " + e.getMessage());
			}
			
			/* Quando termina la fase di extraction si passa alla fase di classificazione */
//			RandomForestModel model = this.classificationDriver.trainAndClassify().;
			
			/* Valuta la classificazione */
			
			/* Aggiorna la base di conoscenza */
			
			/* Stima la nuova frequenza di campionamento */
			
			/* Aggiorna lo snapshot  corrente */
			this.snapshotCounter++;
		}
	}
	
	private void simulateRun() throws IOException {
		RandomForestModel model = null;
		/* Phase 1: Profondità 1 - per farci un'idea. */
		while(this.snapshotCounter < 20) { 
			/* Crawling */
			/* In teoria dovremmo crawlare, ma abbiamo già tutti i dati scaricati.
			
			/* Quando termina il crawling iniziare la fase di feature-extraction */
			try {
				this.featureExtractionDriver.extractFeatures(this.snapshotCounter, true);
			} catch (Exception e) {
				logger.error(this.conf.getWebsite() + " -- " + e.getMessage());
			}
			
			/* Quando termina la fase di extraction si passa alla fase di classificazione. */
			/* In questo caso lo usiamo solo per monitorare dei risultati. */
			model = this.classificationDriver.trainAndClassify(this.snapshotCounter);
			
			/* Stima la nuova frequenza di campionamento */
			/* In questo caso ancora non lo facciamo */
			
			/* Aggiorna lo snapshot corrente */
			this.snapshotCounter++;
		}
		
		/* Popoliamo la base di conoscenza */
		this.knowledgeBase = populateKnowledgeBase(snapshotCounter, model);
		
		/* Phase 2: iniziamo ad ispezionare le sezioni con la base di conoscenza */
		while(!this.terminationCondition) {
			/* Simulate Section driver crawling */
			List<String> effectivelyCrawledURLs = this.crawlingDriver.simulateCrawling(knowledgeBase, this.snapshotCounter);
			
			/* Quando termina il crawling iniziare la fase di feature-extraction */
			try {
				this.featureExtractionDriver.simulateExtractFeatures(effectivelyCrawledURLs, this.snapshotCounter, false);
			} catch (Exception e) {
				logger.error(this.conf.getWebsite() + " -- " + e.getMessage());
			}
			
			model = this.classificationDriver.simulateTrainAndClassify(this.snapshotCounter);
			
			/* Aggiorna base di conoscenza */
			this.knowledgeBase = populateKnowledgeBase(snapshotCounter, model);
			
			/* Valutiamo */
			System.out.println("Sezioni allo snapshot: " + this.snapshotCounter);
			this.knowledgeBase.entrySet().stream()
											.filter(x -> x.getValue().equals(WebsiteClass.section))
											.map(x -> x.getKey())
											.forEach(x -> System.out.println(x));
		}
	}
	
	private Map<String, WebsiteClass> populateKnowledgeBase(int snapshot, RandomForestModel model) throws IOException {
		/* Prendi il "training set" per un determinato snapshot, perchè ci sono scritti i valori delle features */
		String dbName = URLUtils.getDatabaseNameOf(this.conf.getWebsite());
		File csvFile = new File(System.getenv(EnvironmentVariables.datasets) + "/simulation/" + dbName + "/" + dbName + "_" + snapshot + "_training.csv");
				
		return Files.readAllLines(csvFile, StandardCharsets.UTF_8)
			 .stream()
			 .map(line -> line.split(","))
			 .collect(Collectors.toMap(split -> split[0], split -> {
				double[] scores = new double[split.length - 2];
				 for(int i = 1; i < split.length - 2; ++i)
					 scores[i - 1] = Double.parseDouble(split[i]);
				 
				 return WebsiteClass.values()[(int)model.predict(Vectors.dense(scores))];
			 }));
			 
	
	}
	
	
	
	
}
