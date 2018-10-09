package it.uniroma3.classifier;

import java.util.Map;

import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.PageType;
import it.uniroma3.pagerank.PageRank;
import jersey.repackaged.com.google.common.collect.Lists;
import scala.Tuple2;

/**
 * This class can do the page classification of a {@link Graph}.
 * Each node can be assigned to one of the following {@link PageType}:
 * <ol>
 * 	<li>{@link PageType.SECTION}</li>
 * 	<li>{@link PageType.ARTICLE}</li>
 * 	<li>{@link PageType.OTHER}</li>
 * </ol>
 * @author Luigi D'Onofrio
 *
 */
public class PagesClassifier {

	/**
	 * Constructor. 
	 */
	private PagesClassifier() {}
	
	/**
	 * This method starts the classification task. It modifies the {@link Graph} passed as argument.
	 * @param graph is the graph to classify the nodes of.
	 * @throws Exception
	 */
	public static void classify(Graph graph) throws Exception {
		Map<String, Float> ranks = PageRank.compute(graph);
		
		/* Let's find a baseline for Normalization */
		Float bestRank = Lists.newArrayList(ranks.entrySet()).stream()
															 .sorted((e1, e2) -> (-1) * (e1.getValue().compareTo(e2.getValue())))
															 .findFirst()
															 .get()
															 .getValue();
		
		/* Let's mark pages with a Normalized Page Rank under a certain threshold as ARTICLE(s) */
		ranks.entrySet().stream()
						.map(entry -> new Tuple2<>(entry.getKey(), (entry.getValue()/bestRank)))
						.forEach(node -> {
							if(node._2 < 0.55)
								graph.getNode(node._1).setNodeType(PageType.ARTICLE);
						});
		
		/* Let's mark pages marked as OTHER, which doesn't have pointers to any ARTICLE as SECTION */
		graph.getGraphArches().stream()
						.filter(arch -> arch.getFromNode().getNodeType().equals(PageType.OTHER))
						.filter(arch -> arch.getToNode().getNodeType().equals(PageType.ARTICLE))
						.map(arch -> arch.getFromNode())
						.forEach(node -> node.setNodeType(PageType.SECTION));		
	}

}
