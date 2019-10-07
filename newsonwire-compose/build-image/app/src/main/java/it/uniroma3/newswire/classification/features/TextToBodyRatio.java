package it.uniroma3.newswire.classification.features;

import static org.apache.log4j.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Document.OutputSettings.Syntax;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.safety.Whitelist;
import org.spark_project.guava.collect.Lists;

import com.google.common.io.Files;

import it.uniroma3.newswire.persistence.schemas.LinkOccourrences;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import scala.Tuple2;
import scala.Tuple3;

public class TextToBodyRatio extends Feature {
	private static final long serialVersionUID = 8626915061684521608L;
	private String domain;

	public TextToBodyRatio(String dbName, String websiteDomain) {
		super(dbName);
		this.domain = websiteDomain;
	}

	@Override
	public JavaPairRDD<String, Double> calculate(boolean persist, int untilSnapshot) {
		log(INFO, "started");
		
		JavaPairRDD<String, Double> cached = loadCachedData();
		if(cached !=null)
			if(cached.count() != 0) {
			log(INFO, "Data in cache loaded susccessfully: " + cached.count());
			return cached.cache();
		}
		
		/* Erase previous Stability Data */
		erasePreviousResultsData(persist);
		
		long start = System.currentTimeMillis();
		
		
//		if(untilSnapshot > 0)
//			data = data.filter(x -> x._2._4() <= untilSnapshot);
		
		System.out.println("Loading data...");
		JavaPairRDD<String, File> url2path = loadData().filter(x -> x.getInteger(LinkOccourrences.snapshot.name()) <= untilSnapshot)
													   //.mapToPair(x -> new Tuple2<>(x.getString(LinkOccourrences.link.name()), x.getString(LinkOccourrences.file.name())))
													   .map(doc -> {
			String link = doc.getString(LinkOccourrences.link.name());

			String fileName = doc.getString(LinkOccourrences.file.name());
			
			Integer snapshot = doc.getInteger(LinkOccourrences.snapshot.name());
			
			return new Tuple3<>(link, fileName, snapshot);
		})
//				.filter(x -> x._2().exists())
				.mapToPair(x -> new Tuple2<>(x._1(), x._2()))
				.distinct()
				.mapToPair(x -> new Tuple2<>(x._1, new File(x._2)));
					
		long nrOfloaded = url2path.count();
		System.out.println("Data loaded: " + nrOfloaded + "--" + (System.currentTimeMillis() - start) + " ms");																
													    
		
		/*
		 * DEBUG
		 */
//		url2path = url2path.filter(url -> url._1.equals("http://www.ansa.it/piemonte/")).cache();
		AtomicInteger i = new AtomicInteger(0);
		JavaPairRDD<String, Double> results = url2path.mapToPair(tuple -> {
			System.out.println("done: " + i.incrementAndGet() + "/" + nrOfloaded);
			return new Tuple2<>(tuple._1, getTextRoBodyRationScore(tuple._1, tuple._2));
		}).groupByKey()
		  .mapToPair(x -> new Tuple2<>(x._1, Lists.newArrayList(x._2).stream().mapToDouble(y -> y.doubleValue()).average().getAsDouble()))
		  .persist(StorageLevel.MEMORY_ONLY_SER());;
		
		if(persist)
			persist(results.distinct());
		
		log(INFO, "ended.");
		
		return results.distinct();
	}

	private Double getTextRoBodyRationScore(String url, File file) {
		String html = "";
		
		if(!file.exists())
			return -1.0;
		
		try {
			html = Files.toString(file, StandardCharsets.UTF_8).replaceAll("(?m)^[ \t]*\r?\n", "").replaceAll("<\\/strong>|<strong>|<b>|<\\/b>|<i>|<\\/i>|<s>|<\\/s>|<p>|<\\/p>", "");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String domain = this.domain;
		Document doc = Jsoup.parse(Jsoup.clean(html ,"http://" + domain, Whitelist.relaxed().addTags("img").preserveRelativeLinks(true).removeTags("script")));
		doc.outputSettings().escapeMode(EscapeMode.xhtml)
							.syntax(Syntax.xml)
							.charset(StandardCharsets.UTF_8);
		
		/* Convert the previous document into a format parsable by the jOOX library. */
		
		double maximumText = doc.getAllElements().stream()
													   .filter(x -> !x.tagName().equals("body") || !x.tagName().equals("html"))
				   									   .map(textNode -> new Tuple2<>(textNode.text(), (double)textNode.text().length() / (double)textNode.getAllElements().size()))
				   									   .sorted((x,y) -> y._2.compareTo(x._2))
				   									   .findFirst()
				   									   .get()
				   									   ._2;
				   									  
		return new Double(maximumText);
	}

	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score < threshold;
	}
	
	private String modifyPath(String path) {
		return path.replaceAll("app/data", System.getenv(EnvironmentVariables.envData));
	}

}
