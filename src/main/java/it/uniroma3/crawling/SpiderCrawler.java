package it.uniroma3.crawling;

import static it.uniroma3.properties.PropertiesReader.CRAWLER_EXCLUDE_LIST;
import static it.uniroma3.properties.PropertiesReader.CRAWLER_SOCIAL_EXCLUDE_LIST;
import static org.joox.JOOX.$;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
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
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
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
public class SpiderCrawler extends WebCrawler {
	private static final Logger logger = Logger.getLogger(SpiderCrawler.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(" + propsReader.getProperty(CRAWLER_EXCLUDE_LIST).replaceAll(",", "|") + "))$");
	private final static Pattern SOCIAL_FILTERS = Pattern.compile(".+(\\.(" + propsReader.getProperty(CRAWLER_SOCIAL_EXCLUDE_LIST).replaceAll(",", "|") + ")).+");
	private final static Pattern WEB_ARCHIVE_PATTERN = Pattern.compile("^(https?:\\/\\/.*)\\/(https?\\/\\/?www.*)(.*)^|\\/web\\/\\d+\\*?\\/(https?:\\/\\/?)?(.*)");
	private static Connection connection = MySQLRepositoryDAO.getConnection();
	

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		String href = URLCanonicalizer.getCanonicalURL(url.getURL());
//		String domain = "" + ((!page.getWebURL().getSubDomain().equals("")) ? page.getWebURL().getSubDomain() + "." : "") + page.getWebURL().getDomain();

		String domain = referringPage.getWebURL().getDomain();
		Boolean shouldVisit = ((!FILTERS.matcher(href).matches()) && (!SOCIAL_FILTERS.matcher(href).matches()))
								&& href.contains(domain)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("tag") : true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("social"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("user"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("profile"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("eventi"): true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("annunci")	: true)
								&& ((href.contains("www.foggiatoday.it")) ? !href.contains("html")	: true)
								&& ((href.contains("www.rainews.it")) ? !href.contains("articoli"): true)
								&& ((href.contains("www.rainews.it")) ? !href.contains("media"): true)
								//&& ((href.contains("www.ansa.it/games/")) ? false: true) //only one page
								&& ((href.contains("www.wsj.com")) ? !href.contains("/articles/") &&
																	 !href.contains("/author/") &&
																	 !href.contains("/video/") &&
																	 !href.contains("/podcast/") &&
																	 !href.contains("/search/"): true) //only one page
								&& ((href.contains("www.corriere.it")) ? href.matches(".*index\\.shtml|.*\\/"): true)	
								&& ((href.contains("repubblica.it")) ? !href.matches(".+-\\d+.+"): true) && skipURLs(href)
//								&& ((!href.equals("http://www.ansa.it/sito/notizie/cronaca/cronaca.shtml")) ? false : true)
								&& ((href.contains("ilpost.it")) ? !href.matches(".+\\d{4}\\/\\d{2}\\/\\d{2}.+") && skipURLs(href): true);
								
		//return false; <-- useful to download a single page
		return shouldVisit;
	}

	/* (non-Javadoc)
	 * @see edu.uci.ics.crawler4j.crawler.WebCrawler#visit(edu.uci.ics.crawler4j.crawler.Page)
	 */
	@Override
	public void visit(Page page) {
//		String domain = "" + ((!page.getWebURL().getSubDomain().equals("")) ? page.getWebURL().getSubDomain() + "." : "") + page.getWebURL().getDomain();
		String domain = page.getWebURL().getDomain();

		String url = URLCanonicalizer.getCanonicalURL(page.getWebURL().getURL());
		
		
		/* Let's write the visited URL onto the DB. */
		MySQLRepositoryDAO.getInstance().insertURL(connection, url);
		
		/* Parse the HTML */
		if (page.getParseData() instanceof HtmlParseData) {
			/* This is done due to retrieve all the informations about the visited page. */
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			
			/* This is done due to get the XPath of the links in the current page in the next steps. */
			Document doc = Jsoup.parse(Jsoup.clean(htmlParseData.getHtml(),"http://" + domain, Whitelist.relaxed().addTags("img").preserveRelativeLinks(true).removeTags("script")));

			/* Set up all the compliancy shitties */
			doc.outputSettings().escapeMode(EscapeMode.xhtml)
								.syntax(Syntax.xml)
								.charset(StandardCharsets.UTF_8);
			
			/* Convert the previous document into a format parsable by the jOOX library. */
			org.w3c.dom.Document document = $(doc.html()).document();
			Set<WebURL> outgoingLinks = htmlParseData.getOutgoingUrls();
			
			
			outgoingLinks.stream()
						 .filter(x -> x != null)
						 .filter(x -> {
							 String canURL = URLCanonicalizer.getCanonicalURL(x.getURL());
							 return !FILTERS.matcher(canURL).matches()
									 && canURL.contains(domain) 
								     && !SOCIAL_FILTERS.matcher(canURL).matches() 
								     && skipURLs(canURL);
//								     && this.getMyController().getDocIdServer().isSeenBefore(url) ;
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
									
									xpaths.forEach(xpath -> {
										if(!xpath.matches(".+\\/a\\[\\d+\\]"))
											return;
										
										String href = cleanURL(webUrl.getPath());
										String absolute = cleanURL(webUrl.getURL());
										String referringPage = cleanURL(page.getWebURL().getURL());
										
										/* Write both the URL and the annexed link occurrence. */ 
										MySQLRepositoryDAO.getInstance().insertURL(connection, absolute);
										MySQLRepositoryDAO.getInstance().insertLinkOccourrence(connection, absolute, referringPage, href, xpath);
									});
									
						 		});
			logger.info("Number of outgoing links fetched: " +  outgoingLinks.size());
		}
	}
	
	private String preprocessURLForWebArchive(String url) {
		return (url.matches(".+(http:\\/)[a-z].*")) ? url.replaceAll("http:\\/", "http://") : url;
	}
	
	private static String cleanURL(String url) {
		String cleanedURL = (url.startsWith("https://")) ? url.replace("https", "http") : url;
		//cleanedURL = (!cleanedURL.matches(".+www\\..+")) ? cleanedURL.replace("http://", "http://www.") : cleanedURL;
		cleanedURL = (cleanedURL.endsWith("/")) ? cleanedURL.substring(0, cleanedURL.length() -1) : cleanedURL;
		return cleanedURL;
	}
		
	private boolean skipURLs(String url) {
		return ((url.contains("repubblica.it")) ? !url.matches(".+annunci.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+blogautore.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+temi\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+ilmioabbonamento.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+design.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+dizionari.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+dweb.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+miojob.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+trovacinema.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+elezioni\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+oasjs\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+adagiof3\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+alphatest\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+financialounge\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+finanza\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+fantacalcio\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+cityfan\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+meteo\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+notifiche\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+oroscopo.d\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+scuola\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+servizioclienti\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+scripts.kataweb.+"): true)
				
				//TODO: remove this
				&& ((url.contains("repubblica.it")) ? !url.matches(".+video\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+ricerca\\.repubblica.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+xl\\.repubblica.+"): true)
				&& ((url.contains("ilpost.it")) ? !url.matches(".+blog-post.+"): true)
				&& ((url.contains("repubblica.it")) ? !url.matches(".+xl\\.repubblica.+"): true);
		
	}
}