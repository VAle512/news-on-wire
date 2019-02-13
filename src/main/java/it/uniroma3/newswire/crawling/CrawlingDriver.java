package it.uniroma3.newswire.crawling;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_NUM_CRAWLERS;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.CrawlController;
import it.uniroma3.newswire.classification.WebsiteClass;
import it.uniroma3.newswire.persistence.ConcurrentPersister;
import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.properties.PropertiesReader;

/**
 * Questa è la classe che guida la fase di crawling.
 * @author Luigi D'Onofrio
 *
 */
public class CrawlingDriver {
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final int NUM_CRAWLERS = Integer.parseInt(propsReader.getProperty(CRAWLER_NUM_CRAWLERS));
	private static Logger logger = Logger.getLogger(CrawlingDriver.class);
	
	private CrawlingModel model;
	
	/**
	 * Constructor.
	 * @param crawlingModel
	 */
	public CrawlingDriver(CrawlingModel crawlingModel) {
		this.model = crawlingModel;
	}
	
	/**
	 * Inizia l'effettiva fase di crawling per un certo snapshot. La gestione di quest'ultimo è gestito tramite DB.
	 */
	public void startCrawling() {
		ConcurrentPersister concPersister = new ConcurrentPersister();
		concPersister.start();
		
		logger.info("Started crawling " + this.model.getWebsiteRoot());
		
		CrawlController controller = this.model.getMyController();
		controller.start(() -> new Crawler(concPersister), NUM_CRAWLERS);
		
		/* Termina la sessione corrente di crawling */
		if(controller.isFinished()) {
			concPersister.crawlingEnded();
			concPersister.interrupt();
			controller.shutdown();
		}
		
	}
	
	/**
	 * Effettua una simulazione di crawling focused (ecco perchè la Knowledge Base).
	 * @param knowledgeBase
	 * @param snapshot
	 * @return
	 */
	public List<String> simulateCrawling(Map<String, WebsiteClass> knowledgeBase, int snapshot) {
		DAO dao = model.getWebsiteSpecificDAO();
		
		int fromSnapshot = snapshot - 1;
		int toSnapshot = snapshot;
		
		/* Prendiamo tutte le pagine classificate come sezione dalla nostra base di conscenza. */
		List<String> sections = knowledgeBase.entrySet().stream()
														.filter(x -> x.getValue().name().equals(WebsiteClass.section.name()))
														.map(x -> x.getKey())
														.collect(Collectors.toList());
		
		/* Prendiamo tutte le link occurrencies che hanno come referring page le nostre sezioni (e la root) per un certo snapshot. */
		List<String> crawledURLs = dao.getLinkOccurrenciesBeforeSnapshot(sections, fromSnapshot, toSnapshot, false).stream()
																											.map(doc -> doc.getString(link.name()))
																											.collect(Collectors.toList());
		
		crawledURLs.addAll(sections);
		
		return crawledURLs;
		
	}
}
