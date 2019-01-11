package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.file;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static org.apache.log4j.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.spark.api.java.JavaPairRDD;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Document.OutputSettings.Syntax;
import org.jsoup.nodes.Entities.EscapeMode;
import org.jsoup.safety.Whitelist;

import com.google.common.io.Files;

import it.uniroma3.newswire.utils.URLUtils;
import scala.Tuple2;

public class TextToBodyRatio extends Feature {
	private static final long serialVersionUID = 8626915061684521608L;

	public TextToBodyRatio(String dbName) {
		super(dbName);
	}

	@Override
	public JavaPairRDD<String, Double> calculate(boolean persist, int untilSnapshot) {
		log(INFO, "started");
		
		/* Erase previous Stability Data */
		erasePreviousBenchmarkData(persist);
		
		JavaPairRDD<String, File> url2path = loadData().mapToPair(doc -> new Tuple2<>(doc.getString(link.name()), 
																					  new File(doc.getString(file.name()))));
		
		JavaPairRDD<String, Double> results = url2path.mapToPair(tuple -> new Tuple2<>(tuple._1, getTextRoBodyRationScore(tuple._1, tuple._2)));
		
		if(persist)
			persist(results);
		
		log(INFO, "ended.");
		
		return results;
	}

	private Double getTextRoBodyRationScore(String url, File file) {
		String html = "";
		
		try {
			html = Files.toString(file, StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String domain = URLUtils.domainOf(url);
		Document doc = Jsoup.parse(Jsoup.clean(html ,"http://" + domain, Whitelist.relaxed().addTags("img").preserveRelativeLinks(true).removeTags("script")));
		
		doc.outputSettings().escapeMode(EscapeMode.xhtml)
							.syntax(Syntax.xml)
							.charset(StandardCharsets.UTF_8);
		
		long maximumText = doc.getAllElements().stream()
							  				   .mapToLong(textNode -> textNode.text().length())
							  				   .max()
							  				   .getAsLong();
		
		
		return new Double(maximumText);
	}

	@Override
	public boolean isThresholded(Double score, Double threshold) {
		return score < threshold;
	}

}
