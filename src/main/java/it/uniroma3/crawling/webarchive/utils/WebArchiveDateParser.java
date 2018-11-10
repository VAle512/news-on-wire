package it.uniroma3.crawling.webarchive.utils;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebArchiveDateParser {
	private WebArchiveDateParser instance;
	
	public static Date parseDate(String dateToString) {
		Pattern pattern = Pattern.compile("(\\d{4})(\\d{2})(\\d{2})(\\d{1,2})(\\d{2})(\\d{2})");
		Matcher matcher = pattern.matcher(dateToString);
		
		if(matcher.find()) {
			int year = Integer.parseInt(matcher.group(1))  -1900;
			int month = Integer.parseInt(matcher.group(2)) - 1;
			int day = Integer.parseInt(matcher.group(3));
			int hours = Integer.parseInt(matcher.group(4));
			int mins = Integer.parseInt(matcher.group(5));
			int secs = Integer.parseInt(matcher.group(6));
			Date date = new Date(year, month, day, hours, mins, secs);
			return date;
		} else {
			return null;
		}
	}
}
