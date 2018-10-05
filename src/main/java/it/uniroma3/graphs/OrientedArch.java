package it.uniroma3.graphs;

import java.io.Serializable;

public class OrientedArch implements Serializable{
	private static final long serialVersionUID = 5721708311513836988L;
	private Node fromNode;
	private Node toNode;
	
	public OrientedArch(Node from, Node to) {
		this.fromNode = from;
		this.toNode = to;
	}

	public Node getFromNode() {
		return fromNode;
	}

	public void setFromNode(Node fromNode) {
		this.fromNode = fromNode;
	}

	public Node getToNode() {
		return toNode;
	}

	public void setToNode(Node toNode) {
		this.toNode = toNode;
	}
	
	public String toString() {
		return "(" + this.fromNode.toString() + " -> " + this.toNode.toString() + ")";
	}
	
	public String toCouple() {
		return this.fromNode.toString() + " " + this.toNode.toString();
	}
	
	public boolean equals(Object obj) {
		OrientedArch that = (OrientedArch) obj;
		return that.fromNode.equals(this.fromNode) && that.toNode.equals(this.toNode);
	}
}
