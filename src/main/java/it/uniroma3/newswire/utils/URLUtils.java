package it.uniroma3.newswire.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;

public class URLUtils {
	private static Logger logger = Logger.getLogger(URLUtils.class);
	
	public static String getDatabaseNameOf(String url) {
		Pattern dbNamePattern = Pattern.compile("^(?:https?:\\/\\/)?(?:[^@\\/\\n]+@)?(?:www\\.)?([^:\\/?\\n]+)");
		 Matcher matcher = dbNamePattern.matcher(url);
		 if(matcher.find())
			 return matcher.group(1).replaceAll("\\.", "_");
		 else
			 return null;
	}
	
	public static String canonicalize(String url) {
		if(url.startsWith("http"))
			return URLCanonicalizer.getCanonicalURL(url.replace("https://", "http://"));
		else
			return url;
	}
	
	public static String encode(String url) {
		try {
			return URLEncoder.encode(url, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage());
			return null;
		}
	}
	
}
