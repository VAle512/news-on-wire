package it.uniroma3.newswire.utils.xpath;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;

public class XPathElementsGenerator {
	private static final Pattern regex = Pattern.compile("\\/([a-z 1-9]+)\\[(\\d+)\\]");
		
	public static List<Tuple2<String, Integer>> generate(String xpath) {
		List<Tuple2<String,Integer>> elements = new ArrayList<>();
		Matcher matcher = regex.matcher(xpath);
		
		while(matcher.find())
			elements.add(new Tuple2<>(matcher.group(1).toString(), Integer.parseInt(matcher.group(2))));
		
		return elements;		
	}
}
