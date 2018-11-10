package it.uniroma3.crawling;

import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.frontier.DocIDServer;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;

public class CustomDocIDServer extends DocIDServer {

	public CustomDocIDServer(Environment env, CrawlConfig config) {
		super(env, config);
		System.out.println("HELLO");
	}
	
	@Override
	public boolean isSeenBefore(String url) {
		return getDocId(URLCanonicalizer.getCanonicalURL(url))!=-1;
	}

}
