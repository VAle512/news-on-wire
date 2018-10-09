package it.uniroma3.graphs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class models a simple graph representation.
 * Basically a graph is a collection of {@link Node}(s) and {@link Arch}(es).
 *  
 * @author Luigi D'Onofrio
 *
 */
public class Graph implements Serializable {
	private static final long serialVersionUID = 4117186693893093416L;
	private Set<Arch> arches;
	private List<Node> nodes;
	
	/**
	 * Constructor.
	 */
	public Graph() {
		this.arches = new HashSet<>();
		this.nodes = new ArrayList<>();
	}
	
	/**
	 * Adds an oriented {@link Arch} to the graph.
	 * @param from {@link Node}.
	 * @param to {@link Node}.
	 */
	public void addArch(Node from, Node to) {
		Node f = null;
		Node t = null;
		
		/* If the node is already part of this graph, use that instead of a copy instance. */
		if(this.nodes.stream().filter(node -> node.equals(from)).count() > 0) 
			f = this.nodes.stream()
						  .filter(node -> node.equals(from))
						  .findFirst()
						  .get();
			
		if(this.nodes.stream().filter(node -> node.equals(to)).count() > 0) {
			t = this.nodes.stream()
						  .filter(node -> node.equals(to))
						  .findFirst()
						  .get();
		}
		
		Arch arch = new Arch((f==null) ? from: f , (t==null) ? to: t);
		
		if(f==null)
			this.nodes.add(from);
		if(t == null)
			this.nodes.add(to);
		
		this.arches.add(arch);
		
	}
	
	/**
	 * Adds an oriented {@link Arch} to the graph.
	 * @param the {@link Arch} we want to add.
	 */
	public void addArch(Arch arch) {
		this.addArch(arch.getFromNode(), arch.getToNode());
	}
	
	/**
	 * Gets teh node identified by the provided name
	 * @param name is the node name to search for.
	 * @return the corresponding {@link Node}
	 */
	public Node getNode(String url) {
		Node dummyNode = new Node(url);
		return this.nodes.stream().filter(x -> x.equals(dummyNode)).findFirst().get();
	}
	
	/**
	 * This method returns the collection of {@link Arch}(es) of the {@link Graph}.
	 * @return a set of arches
	 */
	public Set<Arch> getGraphArches() {
		return this.arches;
	}
	
	/**
	 * This method returns the collection of {@link Node}(s) of the {@link Graph}.
	 * @return a set of arches
	 */
	public List<Node> getGraphNodes() {
		return this.nodes;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return this.arches.toString();
	}
	
}
