package it.uniroma3.newswire.crawling;

import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import it.uniroma3.newswire.properties.PropertiesReader;

/**
 * Questa classe si occupa di modellare un modello di crawling per uno specifico sito web.
 * La scelta è dettata dal fatto che non si può creare un unico crawler per un insieme di siti, 
 * poichè i valori di tuning che devono essere stimati sono diversi da sito a sito.
 * @author Luigi D'Onofrio
 *
 */
public class CrawlingModel {
	private final PropertiesReader propsReader = PropertiesReader.getInstance();
	private final Logger logger = Logger.getLogger(CrawlingModel.class);
	
	private String websiteRoot;
	private Set<String> seeds;
	private long samplingFrequency;
	private CrawlingDriver myDriver;
	
	/**
	 * Contructor.
	 * @param websiteRoot è  la root del sito che vogliamo aggredire.
	 * @param myDriver è un riferimento al driver che ne controllera il crawling.
	 */
	public CrawlingModel(String websiteRoot, CrawlingDriver myDriver) {
		this.websiteRoot = websiteRoot;
		this.seeds = new HashSet<>();
		
		/* Viene inizializzato ad un valore di default specificato in un file di configurazione */
		this.samplingFrequency = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT)) * 1000;
		
		this.myDriver = myDriver;
		
		logger.info("Crawling Model created successfully for: " + this.websiteRoot);
	}
	
	
	/**
	 * Aggiorna la frequenza di campionamento ad una data.
	 * @param newSamplingFrequency
	 */
	public void updateSamplingFrequency(long newSamplingFrequency) {
		this.samplingFrequency = newSamplingFrequency;
	}


	/**
	 * Restituisce il riferimento a websiteRoot.
	 * @return la websiteRoot
	 */
	public String getWebsiteRoot() {
		return websiteRoot;
	}


	/**
	 * Restituisce il riferimento a seeds.
	 * @return i seeds
	 */
	public Set<String> getSeeds() {
		return seeds;
	}

	/**
	 * Restituisce il riferimento a myDriver.
	 * @return myDriver
	 */
	public CrawlingDriver getMyDriver() {
		return myDriver;
	}


	/**
	 * Restituisce il riferimento a samplingFrequency.
	 * @return samplingFrequency
	 */
	public long getSamplingFrequency() {
		return samplingFrequency;
	}
	
	
}
