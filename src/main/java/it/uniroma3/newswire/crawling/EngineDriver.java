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
import it.uniroma3.newswire.classification.features.DataLoader;
import it.uniroma3.newswire.properties.WebsiteConfiguration;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;

/**
 * Questa classe si occupa di attuare il crawling guidatoio dalle sezioni a due fasi:
 * 	Nella fase 1 operiamo ad una certa profondità (non troppo grande) per isolare il rumore.
 * 	La congettura è che le sezioni vengono pescate tutte anche ad una bassa profondità e sicocme l'obiettivo di questa fase
 * 	è proprio identificarle, in prima istanza, tenderemo a limitarci.
 * 
 *  Nella fase 2 abbiamo già una prima idea di quali che siano le sezioni e pertanto queste diventeranno nuovi entrypoint 
 *  per il crawling andando ad aumentare la profondità di crawling di un'ulteriore unità (o più).
 * @author luigi
 *
 */
public class EngineDriver extends Thread{
	private static final Logger logger = Logger.getLogger(EngineDriver.class);
	private WebsiteConfiguration conf;
	private FeatureExtractionDriver featureExtractionDriver;
	private ClassificationDriver classificationDriver;
	private Map<String, WebsiteClass> knowledgeBase;
	
	private int snapshotCounter;
	
	/**
	 * Constructor.
	 * @param crawlingDriver
	 * @param feDriver
	 * @param classificationDriver
	 * @param conf
	 */
	public EngineDriver(CrawlingDriver crawlingDriver, 
									FeatureExtractionDriver feDriver, 
									ClassificationDriver classificationDriver,
									WebsiteConfiguration conf) {
		this.featureExtractionDriver = feDriver;
		this.classificationDriver = classificationDriver;
		this.snapshotCounter = 2;
		this.conf = conf;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run () {
		try {
			simulateRun();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * Effettua una simulazione di crawling guidato dalle sezioni.
	 * @throws IOException
	 */
	private void simulateRun() throws IOException {
		RandomForestModel model = null;
		
		/* Definiamo una condizione di terminazione. */
		/* TODO: renderle pluggabili con file di configurazione. */
		SnapshotThreshold phaseOneTerminationCondition = new SnapshotThreshold(20);
		phaseOneTerminationCondition.updateCurrentSnapshot(this.snapshotCounter);
		
		/* Phase 1: Profondità 1 - Usiamo questa fase per capire quali sono le sezioni */
		while(phaseOneTerminationCondition.isTerminated()) {
			/* (1) Crawling */
			/* In teoria dovremmo crawlare, ma siccome siamo in una simulazione abbiamo già tutti i dati. */
			
			
			/* (2) Features-Extraction */
			/* Quando termina il crawling iniziare la fase di feature-extraction per lo snapshot selezionato */
			try {
				this.featureExtractionDriver.extractFeatures(this.snapshotCounter, false);
			} catch (Exception e) {
				logger.error(this.conf.getWebsite() + " -- " + e.getMessage());
			}
			
			/* (3) Classification */
			/* Quando termina la fase di extraction si passa alla fase di classificazione. */
			/* In questo caso lo usiamo solo per monitorare dei risultati perchè l'ultiomo model si stima essere il migliore. */
			model = this.classificationDriver.simulateTrainAndClassify(this.snapshotCounter);
			
			/* (4) Stima la nuova frequenza di campionamento. */
			/* In questo caso ancora non lo facciamo */
			
			/* Aggiorna lo snapshot corrente. */
			DataLoader.getInstance().incrementSnapshotTo(snapshotCounter); // Sia sul caricatore di dati.
			this.snapshotCounter++;
			phaseOneTerminationCondition.updateCurrentSnapshot(this.snapshotCounter); // Sia per quanto riguarda la condizione di terminazione.
		}
		
		
		/*
		 * Terminata la fase 1 dobbiamo memorizzare ciò che sappiamo (o che sapopiamo classificare).
		 * Questa operazione è necessaria poichè la fase 2 è guidata dalle sezioni e pertanto 
		 * dobbiamo sapere per ogni iterazioni quali sono le sezioni.
		 */
		this.knowledgeBase = populateKnowledgeBase(model);
		logger.info("Actual Knowledge Base size: " + this.knowledgeBase.size());

		/* Phase 2: iniziamo ad ispezionare le sezioni con la base di conoscenza */
		while(true) {
			/* (1) Simulate Section driver crawling */
			List<String> effectivelyCrawledURLs = this.knowledgeBase.entrySet().stream()
																				.filter(entry -> entry.getValue().name().equals(WebsiteClass.section.name()))
																				.map(x -> x.getKey())
																				.collect(Collectors.toList());
			/* Aggiungiamo anche la root */
			effectivelyCrawledURLs.add(this.conf.getWebsite());
			
			/* Settiamo dei seeds nel caricatore di dati in modo che possa effettuare un'estrazione di features guidata dai dati aappena caricati */ 
			/* TODO: Il DataLoader è singolo, pertanto forse può comportare un problema. */
			DataLoader.getInstance().setSeeds(effectivelyCrawledURLs);
			
			/* (2) Features-Extraction */
			/* Quando termina il crawling iniziare la fase di feature-extraction. */
			try {
				this.featureExtractionDriver.extractFeatures(this.snapshotCounter, false);
			} catch (Exception e) {
				logger.error(this.conf.getWebsite() + " -- " + e.getMessage());
			}
			
			/* (3) Classification */
			/* In questo caso costruiamo un nuovo modello ad ogni iterazione poichè abbiamo bisogno di aggiornare la base di conoscenza. */		
			model = this.classificationDriver.simulateTrainAndClassify(this.snapshotCounter);
			
			/* (4) Aggiorna base di conoscenza */
			this.knowledgeBase = populateKnowledgeBase(model);

			/* (5) Stima della nuova frequenza di campionamento */
			/* in questo caso non ci serve ma sarebbe molto carino vedere come fa */
			// TODO: Estimator.
			
			/* Aggiorna lo snapshot corrente. */
			DataLoader.getInstance().incrementSnapshotTo(snapshotCounter); // Sia sul caricatore di dati.
			this.snapshotCounter++;
			phaseOneTerminationCondition.updateCurrentSnapshot(this.snapshotCounter); // Sia per quanto riguarda la condizione di terminazione.
		}
	}
	
	/**
	 * Effettua una predizione usando il modello passato come parametro ed inserisce i dati all'interno della base di conoscenza.
	 * @param snapshot
	 * @param model
	 * @return
	 * @throws IOException
	 */
	private Map<String, WebsiteClass> populateKnowledgeBase(RandomForestModel model) throws IOException {
		/* Prendi il "training set" per un determinato snapshot, perchè ci sono scritti i valori delle features */
		String dbName = URLUtils.getDatabaseNameOf(this.conf.getWebsite());
		File csvFile = new File(System.getenv(EnvironmentVariables.datasets) + "/simulation/" + dbName + "/" + dbName + "_" + (this.snapshotCounter - 1) + "_training.csv");
		
		return Files.readAllLines(csvFile, StandardCharsets.UTF_8).stream()
																	 .map(line -> line.split(","))
																	 .collect(Collectors.toMap(split -> split[0], split -> {
																		double[] scores = new double[split.length - 2];
																		 for(int i = 1; i < split.length - 1; ++i)
																			 scores[i - 1] = Double.parseDouble(split[i]);
																				 
																		 return WebsiteClass.values()[(int)model.predict(Vectors.dense(scores))];
																	 }));
	}
	
	
	
	
}
