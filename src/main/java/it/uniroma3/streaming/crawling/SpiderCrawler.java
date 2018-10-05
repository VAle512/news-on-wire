package it.uniroma3.streaming.crawling;

import static it.uniroma3.properties.PropertiesReader.GATHERER_DOMAIN;
import static it.uniroma3.properties.PropertiesReader.GATHERER_EXCLUDE_LIST;

import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import it.uniroma3.graphs.Node;
import it.uniroma3.graphs.OrientedArch;
import it.uniroma3.properties.PropertiesReader;
import jersey.repackaged.com.google.common.collect.Sets;

public class SpiderCrawler extends WebCrawler {
	private Set<OrientedArch> arches = Sets.newHashSet();
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final String domain = propsReader.getProperty(GATHERER_DOMAIN);
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(GATHERER_EXCLUDE_LIST).replaceAll(",", "|") + "))$");

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		return !FILTERS.matcher(href).matches()
				&& href.startsWith(domain);
	}

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
				 .forEach(x -> this.arches.add(new OrientedArch(new Node(url), new Node(x))));
			logger.info("Number of outgoing links: " + links.size());
		}
	}
	
	@Override
	public Set<OrientedArch> getMyLocalData() {
		return this.arches;
	}
}