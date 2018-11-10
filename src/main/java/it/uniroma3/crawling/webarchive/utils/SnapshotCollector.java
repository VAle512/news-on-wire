package it.uniroma3.crawling.webarchive.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.crawler4j.crawler.Page;
import jersey.repackaged.com.google.common.collect.Lists;
public class SnapshotCollector {
	private Map<Date, String> date2url;
	private Page snapshotsPage;
	
	public SnapshotCollector(String urlStr) throws Exception {
		StringBuilder result = new StringBuilder();
		Pattern pattern = Pattern.compile("\"ts\":\\[(.*?)\\]");

		URL url = new URL("https://web.archive.org/__wb/calendarcaptures?url=" + urlStr + "&selected_year=2018");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String line;
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		rd.close();

		String resultReq = result.toString();
		Matcher matcher = pattern.matcher(resultReq);
		List<String> snapshots = new ArrayList<>();
		while(matcher.find())
			snapshots.add(matcher.group(1));
			
		this.date2url = snapshots.stream()
				 .map(s -> Arrays.asList(s.split(",")))
				 .flatMap(x -> x.stream())
				 .collect(Collectors.toMap(WebArchiveDateParser::parseDate, (date -> "https://web.archive.org/web/" + date + "/" + urlStr)));
	}

	public Map<Date, String> getAllSnapshots() {
		return this.date2url;
	}
	
	public List<String> getAllSnapshotsURLs() {
		return Lists.newArrayList(this.date2url.values());
	}
	
	public List<String> getSnapshotsInRange(Date from, Date to) {
		List<Date> keys = date2url.keySet().stream().filter(date -> date.after(from) && date.before(to)).collect(Collectors.toList());
		List<String> result = new ArrayList<>();
		
		keys.stream().forEach(key -> result.add(this.date2url.get(key)));
		return result;
	}
}
