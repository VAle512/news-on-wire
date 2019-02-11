package it.uniroma3.newswire.crawling;

import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_DEPTH;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_STORAGE;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;

/**
 * Questa classe si occupa di modellare un modello di crawling per uno specifico sito web.
 * La scelta è dettata dal fatto che non si può creare un unico crawler per un insieme di siti, 
 * poichè i valori di tuning che devono essere stimati sono diversi da sito a sito.
 * @author Luigi D'Onofrio
 *
 */
public class CrawlingModel {
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private final Logger logger = Logger.getLogger(CrawlingModel.class);

	private static final String STORAGE = propsReader.getProperty(CRAWLER_STORAGE);
	private static final int MAX_DEPTH = Integer.parseInt(propsReader.getProperty(CRAWLER_DEPTH));

	private String websiteRoot;
	private Set<String> seeds;
	private long samplingFrequency;
	private CrawlController myController;
	private DAO websiteSpecificDAO;

	/**
	 * Contructor.
	 * @param websiteRoot è  la root del sito che vogliamo aggredire.
	 * @param myController è un riferimento al driver che ne controllera il crawling.
	 */
	public CrawlingModel(String websiteRoot) {
		this.websiteRoot = websiteRoot;
		this.seeds = new HashSet<>();
		this.myController = this.generateCrawlingController();
		this.websiteSpecificDAO = DAOPool.getInstance().getDAO(URLUtils.getDatabaseNameOf(this.websiteRoot));
		
		/* Viene inizializzato ad un valore di default specificato in un file di configurazione */
		this.samplingFrequency = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT)) * 1000;
		
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
	 * Restituisce il riferimento a myController.
	 * @return myController
	 */
	public CrawlController getMyController() {
		return myController;
	}

	/**
	 * Restituisce il riferimento a websiteSpecificDAO.
	 * @return the websiteSpecificDAO
	 */
	public DAO getWebsiteSpecificDAO() {
		return websiteSpecificDAO;
	}

	/**
	 * Restituisce il riferimento a samplingFrequency.
	 * @return samplingFrequency
	 */
	public long getSamplingFrequency() {
		return samplingFrequency;
	}

	/*
	 * Prendiamo tutti i valori di default. Ad un certo punto bisogna migrare verso una soluzione con file specifici per ogni sito.
	 */
	public CrawlController generateCrawlingController() {
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(STORAGE);
		config.setMaxDepthOfCrawling(MAX_DEPTH);

		/* Instantiate the controller for this crawl. */	
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);	

		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = null;

		try {
			controller = new CrawlController(config, pageFetcher, robotstxtServer);
		} catch (Exception e) {
			logger.error(e.getMessage());
			return null;
		}

		controller.addSeed(this.websiteRoot);
		
		return controller;
	}
}
