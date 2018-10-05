package it.uniroma3.graphs;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Graph implements Serializable {
	private static final long serialVersionUID = 4117186693893093416L;
	private String name;
	private Set<OrientedArch> arches;
	private Set<Node> nodes;
	
	public Graph(String name) {
		this.name = name;
		this.arches = new HashSet<>();
		this.nodes = new HashSet<>();
	}
	
	public void addArch(Node from, Node to) {
		OrientedArch arch = new OrientedArch(from, to);
		this.nodes.add(from);
		this.nodes.add(to);
		this.arches.add(arch);
	}
	
	public void addArch(OrientedArch arch) {
		this.nodes.add(arch.getFromNode());
		this.nodes.add(arch.getToNode());
		this.arches.add(arch);
	}
	
	public Node getNode(String url) {
		Node dummyNode = new Node(url);
		return this.nodes.stream().filter(x -> x.equals(dummyNode)).findFirst().get();
	}
	
	public String getName() {
		return name;
	}
	
	public Set<OrientedArch> getGraph() {
		return this.arches;
	}
	
}
