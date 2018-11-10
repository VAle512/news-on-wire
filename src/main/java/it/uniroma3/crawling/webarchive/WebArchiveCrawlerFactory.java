package it.uniroma3.crawling.webarchive;

import edu.uci.ics.crawler4j.crawler.CrawlController.WebCrawlerFactory;

public class WebArchiveCrawlerFactory implements WebCrawlerFactory<WebArchiveCrawler> {
	private String domain;
	
	public WebArchiveCrawlerFactory(String domain) {
		this.domain = domain;
	}

	@Override
	public WebArchiveCrawler newInstance() throws Exception {
		// TODO Auto-generated method stub
		return new WebArchiveCrawler(domain);
	}
	
	

}
