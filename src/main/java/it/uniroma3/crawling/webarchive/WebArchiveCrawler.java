package it.uniroma3.crawling.webarchive;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_EXCLUDE_LIST;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_SOCIAL_EXCLUDE_LIST;
import static org.joox.JOOX.$;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Document.OutputSettings.Syntax;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.safety.Whitelist;
import org.spark_project.guava.io.Files;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.properties.PropertiesReader;

/**
 * This class represents the crawler effectively doing the job.
 * It extends {@link WebCrawler} which is part of the package {@link edu.uci.ics.crawler4j}.
 * 
 * @author Luigi D'Onofrio
 *
 */
public class WebArchiveCrawler extends WebCrawler {
	private static final Logger logger = Logger.getLogger(WebArchiveCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(CRAWLER_EXCLUDE_LIST).replaceAll(",", "|") + "))$");
	private final static Pattern SOCIAL_FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(CRAWLER_SOCIAL_EXCLUDE_LIST).replaceAll(",", "|") + ")).*");
	private final static Pattern WEB_ARCHIVE_PATTERN = Pattern.compile("^(https?:\\/\\/.*)\\/(https?\\/\\/?www.*)(.*)^|\\/web\\/\\d+\\*?\\/(https?:\\/\\/?)?(.*)");
	private static Connection connection = MySQLRepositoryDAO.getConnection();
	private String realDomain;
	
	public WebArchiveCrawler(String domain) {
		this.realDomain = domain;
	}
	
	public WebArchiveCrawler() {}
	
	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#shouldVisit(edu.uci.ics.crawler4j.crawler.Page, edu.uci.ics.crawler4j.url.WebURL)
	 */
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = url.getURL().toLowerCase();
		//String domain = "web." + referringPage.getWebURL().getDomain();
		Boolean shouldVisit = ((!FILTERS.matcher(href).matches()) && (!SOCIAL_FILTERS.matcher(href).matches()))
								&& href.contains(realDomain);
	
		return shouldVisit;
	}

	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#visit(edu.uci.ics.crawler4j.crawler.Page)
	 */
	@Override
	public void visit(Page page) {
		String domain = "" + ((!page.getWebURL().getSubDomain().equals("")) ? page.getWebURL().getSubDomain() + "." : "") + page.getWebURL().getDomain();
		String url = page.getWebURL().getURL();
		
		try {
		url = unifySpace(url);
		}catch(Exception e) {
			return;
		}
		
		/* Let's write the visited URL onto the DB. */
		MySQLRepositoryDAO.getInstance().insertURL(connection, url);
		
		/* Parse the HTML */
		if (page.getParseData() instanceof HtmlParseData) {
			/* This is done due to retrieve all the informations about the visited page. */
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			
			/* This is done due to get the XPath of the links in the current page in the next steps. */
			Document doc = Jsoup.parse(Jsoup.clean(htmlParseData.getHtml(),"http://" + domain, Whitelist.relaxed().preserveRelativeLinks(true).removeTags("script")));
			
			/* Set up all the compliancy shitties */
			doc.outputSettings().escapeMode(EscapeMode.xhtml)
								.syntax(Syntax.xml)
								.charset(StandardCharsets.UTF_8);
			
			/* Convert the previous document into a format parsable by the jOOX library. */
			org.w3c.dom.Document document = $(doc.html()).document();
			Set<WebURL> outgoingLinks = htmlParseData.getOutgoingUrls();
			
			outgoingLinks.stream()
						 .filter(x -> x != null)
						 .filter(x -> !FILTERS.matcher(x.getURL()).matches() && x.getURL().contains(realDomain))
						 .forEach(webUrl -> {
							 		/* Retrieving the XPath of the specific outgoing link .*/ 
									String xpath = null;
									try {	 	
										String relative = webUrl.getPath();
										
										relative = preprocessURLForWebArchive(relative);
										
										xpath = $(document).find(webUrl.getTag()+"[href='" + relative + "']").xpath();
										
										/* If null try with the absolute path */
										if(xpath == null)
											xpath = $(document).find(webUrl.getTag()+"[href='" + webUrl.getURL() + "']").xpath();
										
										String href = preprocessURLForWebArchive(unifySpace(webUrl.getPath()));
										String absolute = preprocessURLForWebArchive(unifySpace(webUrl.getURL()));
										String referringPage = preprocessURLForWebArchive(unifySpace(page.getWebURL().getURL()));
										
										/* Write both the URL and the annexed link occurrence. */ 
										MySQLRepositoryDAO.getInstance().insertURL(connection, absolute);
										MySQLRepositoryDAO.getInstance().insertLinkOccourrence(connection, absolute, referringPage, href, xpath);
									
									} catch (Exception e) {
										//logger.info(e.getMessage() + " for URL: " + webUrl.getURL());
									}
						 		});
			logger.info("Number of outgoing links fetched: " +  outgoingLinks.size());
		}
	}
	
	private String preprocessURLForWebArchive(String url) {
		/* Correnct http protocol */
		url = (url.matches(".+(http:\\/)[a-z].*")) ? url.replaceAll("http:\\/", "http://") : url;
		/* Remove port from urls */
		url = (url.matches(".+:\\d+.+")) ? url.replaceAll(":\\d+", "") : url;
		return url;
	}
	
	//TODO: Da rivedere ma per ora funziona
	private static String unifySpace(String url) throws Exception {
		Matcher matcher = WEB_ARCHIVE_PATTERN.matcher(url);
		
		if(matcher.find())
			return matcher.group(5);			
		else
			throw new Exception();
	}
}