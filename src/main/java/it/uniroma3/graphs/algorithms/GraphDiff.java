package it.uniroma3.graphs.algorithms;
import static it.uniroma3.properties.PropertiesReader.SPARK_REPARTITION_LEVEL;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.uniroma3.graphs.Arch;
import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.Node;
import it.uniroma3.properties.PropertiesReader;
import it.uniroma3.spark.SparkLoader;
import jersey.repackaged.com.google.common.collect.Lists;
import scala.Tuple2;

/**
 * This class can do the diff analysis of two {@link Graph}(s). 
 * It can determine the <i>"moves"</i> done in the transition between the twos.
 * What could happen is:
 * <ol>
 * 	<li>A {@link Node} pointer has been removed.</li>
 * 	<li>A {@link Node} pointer has been moved to another (or more) {@link Node}.
 * 	<li>A new {@link Node} is added.</li>
 * </ol>
 * @author Luigi D'Onofrio
 *
 */
public class GraphDiff {
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final Integer sparkRepartitionLevel = Integer.parseInt(propsReader.getProperty(SPARK_REPARTITION_LEVEL));
	private static List<Arch> moved = new ArrayList<>();
	private static List<Node> removed = new ArrayList<>();
	private static List<Node> newNodes = new ArrayList<>();
	
	/**
	 * This method executes the difference analysis on two {@link Graph}(s).
	 * The results are stored in static variables.
	 * @param graph1
	 * @param graph2
	 */
	public static void diff(Graph graph1, Graph graph2) {
		/* Load the Spark Context */
		JavaSparkContext sc = SparkLoader.getInstance().getContext();
		List<Arch> archesGraph1 = Lists.newArrayList(graph1.getGraphArches());
		List<Arch> archesGraph2 = Lists.newArrayList(graph2.getGraphArches());
		
		/* Parallelize the two lists to get two RDDs */
		JavaRDD<Tuple2<String,String>> rddGraph1 = sc.parallelize(archesGraph1, sparkRepartitionLevel).map(arch -> new Tuple2<>(arch.getFromNode().getName(), arch.getToNode().getName())).cache();
		JavaRDD<Tuple2<String,String>> rddGraph2 = sc.parallelize(archesGraph2, sparkRepartitionLevel).map(arch -> new Tuple2<>(arch.getFromNode().getName(), arch.getToNode().getName())).cache();
		
		/* 
		 * The idea is to apply the difference between the arches.
		 * The remaining arches are the ones that need further analysis.
		 * If two graphs are the same, the difference operation will have no effects.
		 */
		rddGraph1.union(rddGraph2)
			.subtract(rddGraph1.intersection(rddGraph2))
			.map(tuple -> new Arch(new Node(tuple._1), new Node(tuple._2)))
			.foreach(arch -> {
				/* 
				 * If a node is contained in both the graphs but it belongs to an arch 
				 * which survived the difference operation, we are sure that node has been moved. 
				 */
				if(graph1.getGraphNodes().contains(arch.getToNode()) && graph2.getGraphNodes().contains(arch.getToNode()) ) {
					if(graph2.getGraphArches().contains(arch))
						moved.add(arch);	
				/* 
				 * If a node is NOT contained in both the graphs and it belongs to an arch of the first graph
				 * we are sure that node has been removed. 
				 */
				} else if(graph1.getGraphArches().contains(arch) && !graph2.getGraphArches().contains(arch)) {
						removed.add(arch.getToNode());
				/* 
				 * If a node is NOT contained in both the graphs and it belongs to an arch of the second graph
				 * we are sure that node has been added. 
				 */
				}else if (!graph1.getGraphArches().contains(arch) && graph2.getGraphArches().contains(arch)) {
						newNodes.add(arch.getToNode());
				}
			});
				
			printResults();
	}
			
	private static void printResults() {
		System.out.println("New Nodes: " + newNodes);
		System.out.println("Removed Nodes: " + removed);
		System.out.println("Moved: " + moved);
	}

}
