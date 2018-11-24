package it.uniroma3.newswire.crawling;

import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_EXCLUDE_LIST;
import static it.uniroma3.newswire.utils.URLUtils.canonicalize;
import static org.joox.JOOX.$;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.PageSaver;
import it.uniroma3.newswire.utils.URLUtils;

/**
 * This class represents the crawler effectively doing the job.
 * It extends {@link WebCrawler} which is part of the package {@link edu.uci.ics.crawler4j}.
 * 
 * @author Luigi D'Onofrio
 *
 */
public class Crawler extends WebCrawler {
	private static final Logger logger = Logger.getLogger(Crawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private final static Pattern FILE_FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(CRAWLER_EXCLUDE_LIST).replaceAll(",", "|") + "))$");
	private final static boolean isPersistEnabled = true;
	
	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#shouldVisit(edu.uci.ics.crawler4j.crawler.Page, edu.uci.ics.crawler4j.url.WebURL)
	 */
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = canonicalize(url.getURL());
		String domain = referringPage.getWebURL().getDomain();
		return shouldBeVisited(href, domain);
	}
	
	//TODO: Enable it on another crawl cycle. 
//	@Override
//	protected WebURL handleUrlBeforeProcess(WebURL curURL) {
//		curURL.setURL(URLUtils.encode(curURL.getURL()));
//		return curURL;
//	}
	
	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#visit(edu.uci.ics.crawler4j.crawler.Page)
	 */
	@Override
	public void visit(Page page) {
		String domain = page.getWebURL().getDomain();
		
		/* Parse the HTML */
		if (!(page.getParseData() instanceof HtmlParseData))
			return;
		
		/* This is done due to retrieve all the informations about the visited page. */
		HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			
		/* If needed download the page too. */
		if(isPersistEnabled)
			PageSaver.savePageOnFileSystem(page);
		
		/* This is done due to get the XPath of the links in the current page in the next steps. */
		Document doc = Jsoup.parse(Jsoup.clean(htmlParseData.getHtml(),"http://" + domain, Whitelist.relaxed().addTags("img").preserveRelativeLinks(true).removeTags("script")));

		/* Set up all the compliancy shitties */
		doc.outputSettings().escapeMode(EscapeMode.xhtml)
							.syntax(Syntax.xml)
							.charset(StandardCharsets.UTF_8);
			
		/* Convert the previous document into a format parsable by the jOOX library. */
		org.w3c.dom.Document document = $(doc.html()).document();
		Set<WebURL> outgoingLinks = htmlParseData.getOutgoingUrls();
			
		String domaninForDAO = URLUtils.domainOf(page.getWebURL().getURL());
		Connection connection = DAOPool.getInstance().getDAO(domaninForDAO).getConnection();
		
		outgoingLinks.stream()
					 .filter(x -> x != null)
					 .filter(x -> {
						 String canURL = canonicalize(x.getURL());
						 return shouldBeVisited(canURL, domain);
					  })								      
					 .forEach(webUrl -> {
						 		/* Retrieving the XPath of the specific outgoing link .*/ 
								List<String> xpaths = new ArrayList<>();
								try {	 	
									String relative = webUrl.getPath();

									xpaths = $(document).find(webUrl.getTag()+"[href='" + relative + "']:only-child").xpaths();
									
									/* If null try with the absolute path */
									if(xpaths.isEmpty())
										xpaths = $(document).find(webUrl.getTag()+"[href='" + webUrl.getURL() + "']:only-child").xpaths();
								
								} catch (Exception e) {
									logger.info(e.getMessage() + " for URL: " + webUrl.getURL());
								}
								
								String href = webUrl.getPath();
								String absolute = canonicalize(webUrl.getURL());
								String referringPage = canonicalize(page.getWebURL().getURL());
								
								xpaths.forEach(xpath -> {
									if(!xpath.matches(".+\\/a\\[\\d+\\]"))
										return;							
									DAOPool.getInstance().getDAO(domaninForDAO).insertLinkOccourrence(connection, absolute, referringPage, href, xpath);
								});
								
					 		});
			try {
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
			logger.info("Number of outgoing links fetched: " +  outgoingLinks.size());
	}
	
	private boolean shouldBeVisited(String url, String domain) {
		return ((!FILE_FILTERS.matcher(url).matches()) && url.matches(("http:\\/\\/(www)?\\." + domain +".+")));
	}
}