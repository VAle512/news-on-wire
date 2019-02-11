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
import it.uniroma3.newswire.persistence.schemas.LinkOccourrences;
import it.uniroma3.newswire.properties.PropertiesReader;

public class CrawlingDriver {
	private CrawlingModel model;
	
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private Logger logger = Logger.getLogger(CrawlingDriver.class);

	private static final int NUM_CRAWLERS = Integer.parseInt(propsReader.getProperty(CRAWLER_NUM_CRAWLERS));

	public CrawlingDriver(CrawlingModel websiteCrawlingModel) {
		this.model = websiteCrawlingModel;
	}
	
	public void startCrawling() {
		ConcurrentPersister concPersister = new ConcurrentPersister();
		concPersister.start();
		
		logger.info("Started crawling " + this.model.getWebsiteRoot());
		
		CrawlController controller = this.model.getMyController();
		controller.start(() -> new Crawler(concPersister), NUM_CRAWLERS);
		
		/* Termina la sessione corrente di crawling */
		if(controller.isFinished()) { // Should be blocking in case of Non-Blocking crawling
			concPersister.crawlingEnded();
			concPersister.interrupt();
			controller.shutdown();
		}
		
	}
	
	public List<String> simulateCrawling(Map<String, WebsiteClass> knowledgeBase, int snapshot) {
		DAO dao = model.getWebsiteSpecificDAO();
		
		int fromSnapshot = snapshot - 1;
		int toSnapshot = snapshot;
		List<String> crawledURLs = dao.getLinkOccurrenciesBeforeSnapshot(fromSnapshot, toSnapshot, false).stream()
																											.map(doc -> doc.getString(link.name()))
																											.collect(Collectors.toList());
		
		crawledURLs = crawledURLs.parallelStream().filter(url -> {
			if(knowledgeBase.containsKey(url))
				return knowledgeBase.get(url).equals(WebsiteClass.section);
			else
				return true;
		}).collect(Collectors.toList());
		
		return crawledURLs;
		
	}
}
