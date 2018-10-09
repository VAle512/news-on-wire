package it.uniroma3.crawling;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_DOMAIN;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_EXCLUDE_LIST;

import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import it.uniroma3.graphs.Node;
import it.uniroma3.graphs.Arch;
import it.uniroma3.properties.PropertiesReader;
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * This class represents the crawler effectively doing the job.
 * It extends {@link WebCrawler} which is part of the package {@link edu.uci.ics.crawler4j}.
 * 
 * @author Luigi D'Onofrio
 *
 */
public class SpiderCrawler extends WebCrawler {
	private Set<Arch> arches = Sets.newHashSet();
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final String domain = propsReader.getProperty(CRAWLER_DOMAIN);
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(CRAWLER_EXCLUDE_LIST).replaceAll(",", "|") + "))$");
	
	
	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#shouldVisit(edu.uci.ics.crawler4j.crawler.Page, edu.uci.ics.crawler4j.url.WebURL)
	 */
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		return !FILTERS.matcher(href).matches()
				&& href.startsWith(domain);
	}

	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#visit(edu.uci.ics.crawler4j.crawler.Page)
	 */
	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			Set<WebURL> links = htmlParseData.getOutgoingUrls();
			links.stream()
				 .filter(x -> x != null)
				 .map(webUrl -> webUrl.getURL())
				 .filter(x -> !FILTERS.matcher(x).matches() && x.startsWith(domain))
				 .forEach(x -> this.arches.add(new Arch(new Node(url), new Node(x))));
			logger.info("Number of outgoing links for " + links.size());
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#getMyLocalData()
	 */
	@Override
	public Set<Arch> getMyLocalData() {
		return this.arches;
	}
}