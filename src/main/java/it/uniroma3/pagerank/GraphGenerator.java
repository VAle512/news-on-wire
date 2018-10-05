package it.uniroma3.pagerank;

import static it.uniroma3.properties.PropertiesReader.PAGE_RANK_GRAPH_GENERATOR_DEPTH;
import static it.uniroma3.properties.PropertiesReader.PAGE_RANK_GRAPH_GENERATOR_FILE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.google.common.io.Files;

import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.Node;
import it.uniroma3.graphs.OrientedArch;
import it.uniroma3.properties.PropertiesReader;
import it.uniroma3.streaming.utils.WGetForURLs;
import jersey.repackaged.com.google.common.collect.Sets;

public class GraphGenerator {
	private static final Logger logger = Logger.getLogger(GraphGenerator.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final File file = new File(propsReader.getProperty(PAGE_RANK_GRAPH_GENERATOR_FILE));
	private static final boolean writeOnFile = true;
	private static final int depth = Integer.parseInt(propsReader.getProperty(PAGE_RANK_GRAPH_GENERATOR_DEPTH));
	
	private GraphGenerator() {}
	
	public static Graph generateGraph(String website) {
		long start = System.currentTimeMillis();
		logger.info("Generating graph from: " + website);
		logger.info("Depth: " + depth);
		Set<OrientedArch> arches = getArches(website, depth);
		final Graph graph = new Graph(website);
		arches.stream().forEach(arch -> {
										graph.addArch(arch);
										if(writeOnFile) {
											try {
												Files.append(arch.toCouple(), file, StandardCharsets.UTF_8);
												Files.append("\n", file, StandardCharsets.UTF_8);
											}catch(Exception e) {
												logger.error("Error while writing on file!");
											}
										}
								});
		logger.info("Finished generating graph in " + TimeUnit.MINUTES.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS) + " minutes.");
		return graph;
	}
	
	private static Set<OrientedArch> getArches(String nodeLink, int depth){
		/* base-case: return emtpy set */
		if(depth == 0) {
			return Sets.newHashSet();
		}
		
		logger.info("Exploring depth: " + depth);
		Set<String> urls = null;
		try {
			urls = WGetForURLs.getPageURLs(nodeLink);
		} catch (ClassNotFoundException 
				| IOException 
				| InterruptedException e) {
			logger.error(e.getMessage());
		}
		
		Node thisNode = new Node(nodeLink);
		Set<OrientedArch> currentArches = urls.parallelStream()
											  .map(url -> new OrientedArch(thisNode, new Node(url)))
											  .collect(Collectors.toSet());
		
		currentArches.addAll(urls.stream()
				   	 .map(url -> getArches(url, (depth-1)))
				   	 .flatMap(x -> x.stream())
				   	 .collect(Collectors.toSet()));
		
		return currentArches;

	}
}
