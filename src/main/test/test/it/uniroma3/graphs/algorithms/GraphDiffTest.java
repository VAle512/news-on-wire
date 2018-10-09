/**
 * 
 */
package test.it.uniroma3.graphs.algorithms;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import it.uniroma3.graphs.Arch;
import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.Node;
import it.uniroma3.graphs.algorithms.GraphDiff;

/**
 * @author Luigi D'Onofrio
 *
 */
public class GraphDiffTest {
	private Graph graphA;
	private Graph graphB;
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {		
		this.graphA = new Graph();
		this.graphB = new Graph();
	}

	@Test
	public void testDiff_EmptyGraphs() {
		Graph graph1 = new Graph();
		Graph graph2 = new Graph();
		
		Node nodeH = new Node("H");
		Node nodeA = new Node("A");
		Node nodeB = new Node("B");
		Node nodeC = new Node("C");
		Node node1 = new Node("1");
		Node node2 = new Node("2");
		Node node3 = new Node("3");
		Node node4 = new Node("4");
		Node node5 = new Node("5");
		Node node6 = new Node("6");
		
		graph1.addArch(new Arch(nodeH, nodeA));
		graph1.addArch(new Arch(nodeH, nodeB));
		graph1.addArch(new Arch(nodeA, node1));
		graph1.addArch(new Arch(nodeA, node2));
		graph1.addArch(new Arch(nodeA, node3));
		graph1.addArch(new Arch(nodeB, node4));
		//graph1.addArch(new Arch(nodeC, node5));
		
		graph2.addArch(new Arch(nodeH, nodeA));
		graph2.addArch(new Arch(nodeH, nodeB));
		//graph2.addArch(new Arch(nodeA, node1));
		graph2.addArch(new Arch(nodeA, node2));
		graph2.addArch(new Arch(nodeB, node3));
		graph2.addArch(new Arch(nodeB, node4));
		graph2.addArch(new Arch(nodeC, node6));

		
		GraphDiff.diff(graph1, graph2);
		
	}
	
	@Test
	public void testDiff_SameGraph() {

	}
	
	@Test
	public void testDiff_OneDeleted() {

	}
	
	@Test
	public void testDiff_OneMoved() {

	}
	
	@Test
	public void testDiff_OneNewNode() {

	}

}
