package it.uniroma3.newswire.benchmark.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.spark_project.guava.collect.Sets;

import it.uniroma3.newswire.utils.xpath.XPath;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import scala.Tuple2;
import scala.Tuple4;

/**
 * Wonky class to find link collections.
 * @author Luigi D'Onofrio
 *
 */
public class LinkCollectionFinder {
	@SuppressWarnings("unchecked")
	public static List<Tuple2<String,  Set<Long>>> findCollections(List<Tuple4<Long,String, XPath, Integer>> quad) {
		System.out.println("Started link collection calculation...");
		Map<String, Map<Integer, List<Tuple4<Long,String, XPath, Integer>>>> referringSnapshot2XPaths = quad.stream()
																								  			   .collect(Collectors.groupingBy(Tuple4<Long,String, XPath, Integer>::_2, 
																								  						Collectors.groupingBy(Tuple4<Long,String, XPath, Integer>::_4)));
		List<Set<Tuple4<Long,String, XPath, Integer>>> result = new ArrayList<>();
		try(ProgressBar pb = new ProgressBar("Finding collections", quad.size(), ProgressBarStyle.UNICODE_BLOCK)){
			
			for(Entry<String, Map<Integer, List<Tuple4<Long,String, XPath, Integer>>>> referringPage: referringSnapshot2XPaths.entrySet()) {
				/* Here we have all the (snapshot, list) of the same referring page */
				
				for(List<Tuple4<Long,String, XPath, Integer>> relatedXpaths: referringPage.getValue().values()) {
					/* Here we have all the xpaths of the same snapshot. */
					List<Set<Tuple4<Long,String, XPath, Integer>>> partialResult = new ArrayList<>();
					for(Tuple4<Long,String, XPath, Integer> current: relatedXpaths) {
						boolean siblingFound = false;
						
						for(Set<Tuple4<Long,String, XPath, Integer>> entryResult: partialResult) {
							XPath that = entryResult.stream().findFirst().get()._3();
							if(current._3().isSameCollection(that)) {
								siblingFound = true;
								entryResult.add(current);
								break;
							}
						}
						
						if(!siblingFound) {
							partialResult.add(Sets.newHashSet(current));
						}
					}
					
					pb.stepBy(partialResult.size());
					result.addAll(partialResult);
					
				}
	
			} 
		}
		System.out.println("Link collection calculation finished.");

		return result.stream().map(set -> new Tuple2<String, Set<Long>>(calculateCollectionXPath(set.stream().map(x -> x._3()).collect(Collectors.toSet())), 
				                                       					   set.stream().map(x -> x._1()).collect(Collectors.toSet())))
							  .collect(Collectors.toList());
	}
	
	private static String calculateCollectionXPath(Set<XPath> xpaths) {
		String firstXpath = xpaths.stream().findFirst().get().xpath;
		StringBuilder builder = new StringBuilder(firstXpath);
		try {
			for(String xpath: xpaths.stream().map(x -> x.xpath).collect(Collectors.toSet()))
				for(int i=0; i < builder.length(); ++i)
					if(xpath.charAt(i)!=builder.charAt(i)) {
						builder.setCharAt(i, '*');
						builder.setCharAt((i+1), ']');
						builder.setLength(i+2);
						break;
					}
		} catch (Exception e) {
			System.out.println(xpaths);
		}
		
		return builder.toString();	
	}
}
