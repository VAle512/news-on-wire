package it.uniroma3.crawling;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_EXCLUDE_LIST;
import static org.joox.JOOX.$;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Document.OutputSettings.Syntax;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.safety.Whitelist;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import it.uniroma3.graphs.Arch;
import it.uniroma3.graphs.Node;
import it.uniroma3.persistence.MySQLRepositoryDAO;
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
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(CRAWLER_EXCLUDE_LIST).replaceAll(",", "|") + "))$");
	private static Connection connection = MySQLRepositoryDAO.getInstance().getConnection();
	

	
	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#shouldVisit(edu.uci.ics.crawler4j.crawler.Page, edu.uci.ics.crawler4j.url.WebURL)
	 */
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		String domain = "www." + referringPage.getWebURL().getDomain();
		Boolean shouldVisit = (!FILTERS.matcher(href).matches())
								&& href.contains(domain)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("tag") : true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("social"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("user"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("profile"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("eventi"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("annunci")	: true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains(".html")	: true)
								&& ((href.contains("www.rainews.it")) ? !href.contains("articoli"): true)
								&& ((href.contains("www.rainews.it")) ? !href.contains("media"): true)
								&& ((href.contains("www.ansa.it")) ? href.matches(".*index\\.html|.*\\/|.*\\.shtml|.*\\/\\?.*"): true)
								&& ((href.contains("www.corriere.it")) ? href.matches(".*index\\.shtml|.*\\/|.*\\/\\?.*"): true);	
		return shouldVisit;

		
	}

	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#visit(edu.uci.ics.crawler4j.crawler.Page)
	 */
	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		String domain = "http://"+page.getWebURL().getDomain();
		//int snapshot = RepositoryDAO.getInstance().getCurrentSequence();
		MySQLRepositoryDAO.getInstance().insertURL(connection, url);
		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			Document doc = Jsoup.parse(Jsoup.clean(htmlParseData.getHtml(), domain, Whitelist.relaxed().preserveRelativeLinks(true)));
			
			doc.outputSettings().escapeMode(EscapeMode.xhtml)
								.syntax(Syntax.xml)
								.charset(StandardCharsets.UTF_8);
			
			org.w3c.dom.Document document = $(doc.html()).document();
			Set<WebURL> links = htmlParseData.getOutgoingUrls();
			links.stream()
				 .filter(x -> x != null)
				 .filter(x -> !FILTERS.matcher(x.getURL()).matches() && x.getURL().contains(domain))
				 .map(webUrl -> {
					 String xpath = null;
					 try {
						 xpath = $(document).find(webUrl.getTag()+"[href='"+webUrl.getPath()+"']").xpath();
					 } catch (Exception e) {
						 logger.info(e.getMessage() + " for URL: " + webUrl.getURL());
					 }
					 String href = webUrl.getPath();
					 String absolute = webUrl.getURL();
					 MySQLRepositoryDAO.getInstance().insertURL(connection, url);
					 MySQLRepositoryDAO.getInstance().insertLinkOccourrence(connection, absolute, page.getWebURL().getURL(), href, xpath);
					return webUrl.getURL(); 
				 })		 
				 .forEach(x -> this.arches.add(new Arch(new Node(url), new Node(x))));
			logger.info("Number of outgoing links fetched: " +  links.size() + ".");
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