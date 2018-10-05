package it.uniroma3.streaming.utils;
import static it.uniroma3.properties.PropertiesReader.GATHERER_DEPTH_LEVEL;
import static it.uniroma3.properties.PropertiesReader.GATHERER_ENTRY_POINT;
import static it.uniroma3.properties.PropertiesReader.GATHERER_EXCLUDE_LIST;
import static it.uniroma3.properties.PropertiesReader.GATHERER_TIMEOUT_KILL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

import it.uniroma3.persistence.RepositoryDAO;
import it.uniroma3.properties.PropertiesReader;
import it.uniroma3.streaming.kafka.URLProducer;

public class WGetForURLs {
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	
	private static final Pattern extractURL = Pattern.compile("--(.*)\\s(.*)--\\s+(.*)");
	private static final String EXCLUDE = ".*\\.(" + propsReader.getProperty(GATHERER_EXCLUDE_LIST).replace(",", "|") +")$";
	private static final String SPACE = " ";
	private static final String TIMEOUT = "timeout -s KILL " + propsReader.getProperty(GATHERER_TIMEOUT_KILL);
	private static final String SPIDER = "--spider";
	private static final String ASK_FOR_LAST_MODIFIED = "--timestamping";
	private static final String RECURSIVE = "-rH";
	private static final String DOMAIN = "-Dwww.ansa.it";
	private static final String DEPTH = "--level=";
	private static final String ENTRY_POINT = propsReader.getProperty(GATHERER_ENTRY_POINT);
	private static final String STDERR_TO_STDOUT = "2>&1";
	private static final String WGET = "wget";
	private static String wgetCommand;
	
	private static void buildCommand() {
		StringBuilder stringBuilder = new StringBuilder();
		if(propsReader.getProperty(GATHERER_TIMEOUT_KILL) != null)
			stringBuilder.append(TIMEOUT).append(SPACE);
		stringBuilder.append(WGET).append(SPACE);
		stringBuilder.append(SPIDER).append(SPACE);
		stringBuilder.append(RECURSIVE).append(SPACE);
		if(propsReader.getProperty(GATHERER_DEPTH_LEVEL) != null) {
			String Depth = propsReader.getProperty(GATHERER_DEPTH_LEVEL);
			stringBuilder.append(DEPTH + Depth).append(SPACE);
		}
		stringBuilder.append(ENTRY_POINT).append(SPACE);
		stringBuilder.append(STDERR_TO_STDOUT).append(SPACE);
		
		wgetCommand = stringBuilder.toString();
	}
	
	private static void buildCommand(int depth) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(WGET).append(SPACE);
		stringBuilder.append(SPIDER).append(SPACE);
		stringBuilder.append(RECURSIVE).append(SPACE);
		stringBuilder.append(DEPTH + depth).append(SPACE);
		stringBuilder.append(ENTRY_POINT).append(SPACE);
		stringBuilder.append(STDERR_TO_STDOUT).append(SPACE);
		
		wgetCommand = stringBuilder.toString();
	}
	
	private static void buildCommand(String url) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(WGET).append(SPACE);
		stringBuilder.append(SPIDER).append(SPACE);
		stringBuilder.append(RECURSIVE).append(SPACE);
		stringBuilder.append(DOMAIN).append(SPACE);
		stringBuilder.append(DEPTH + 1).append(SPACE);
		stringBuilder.append(url).append(SPACE);
		stringBuilder.append(STDERR_TO_STDOUT).append(SPACE);
		
		wgetCommand = stringBuilder.toString();
		System.out.println(wgetCommand);
	}
	public static void explore() throws IOException, InterruptedException, ClassNotFoundException {
		WGetForURLs.buildCommand();
		get();
	}
	
	public static void explore(int depth) throws IOException, InterruptedException, ClassNotFoundException {
		WGetForURLs.buildCommand(depth);
		get();
	}
	
	public static void get() throws IOException, ClassNotFoundException, InterruptedException {
		List<String> tokens = Lists.newArrayList(wgetCommand.split("\\s"));
		ProcessBuilder builder = new ProcessBuilder(tokens);
		Process p = builder.redirectErrorStream(true).start();
		
		URLProducer producer = new URLProducer();
		
		InputStreamReader istream = new  InputStreamReader(p.getInputStream());
		BufferedReader br = new BufferedReader(istream);

		String line;
		Matcher matcher;
		
		int snapshotCounter = RepositoryDAO.getInstance().getNextSequence();
		while ((line = br.readLine()) != null){
			if(line.startsWith("--")) {
				matcher = extractURL.matcher(line);
				if(matcher.find()) {
					String url = matcher.group(3);
					if(!url.matches(EXCLUDE)) {
						producer.produceMessage(url+"$$"+snapshotCounter);
					}
						
				}			
			}
		}

		p.waitFor();
		producer.close();
	}
	
	public static Set<String> getUrls() throws IOException, ClassNotFoundException, InterruptedException {
		Set<String> urls = new HashSet<>();
		List<String> tokens = Lists.newArrayList(wgetCommand.split("\\s"));
		ProcessBuilder builder = new ProcessBuilder(tokens);
		Process p = builder.redirectErrorStream(true).start();
				
		InputStreamReader istream = new  InputStreamReader(p.getInputStream());
		BufferedReader br = new BufferedReader(istream);

		String line;
		Matcher matcher;
		
		while ((line = br.readLine()) != null){
			if(line.startsWith("--")) {
				matcher = extractURL.matcher(line);
				if(matcher.find()) {
					String url = matcher.group(3);
					if(!url.matches(EXCLUDE)) {
						urls.add(url);
					}
						
				}			
			}
		}

		p.waitFor();
		return urls;
	}
	
	public static Set<String> getPageURLs(String page) throws ClassNotFoundException, IOException, InterruptedException {
		WGetForURLs.buildCommand(page);
		return getUrls();
	}

}
