package it.uniroma3.graphs;

import java.io.Serializable;

/**
 * This class models an arch of a {@link Graph}. In particular this is an <b>oriented</b> arch.
 * @author Luigi D'Onofrio
 *
 */
public class Arch implements Serializable{
	private static final long serialVersionUID = 5721708311513836988L;
	private Node fromNode;
	private Node toNode;
	
	/**
	 * Constructor.
	 * @param from is the node the arch starts from
	 * @param to is the destination node of the arch
	 */
	public Arch(Node from, Node to) {
		this.fromNode = from;
		this.toNode = to;
	}

	/**
	 * @return the left hand side of the arch.
	 */
	public Node getFromNode() {
		return fromNode;
	}

	/**
	 * @return the right hand side of the arch.
	 */
	public Node getToNode() {
		return toNode;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "(" + this.fromNode.toString() + " -> " + this.toNode.toString() + ")";
	}
	
	/**
	 * @return a String representation which can be written on a file. 
	 * This is a very common representation.
	 * 
	 */
	public String toCouple() {
		return this.fromNode.toString() + " " + this.toNode.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		Arch that = (Arch) obj;
		return that.fromNode.equals(this.fromNode) && that.toNode.equals(this.toNode);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return this.fromNode.getName().hashCode() + this.toNode.getName().hashCode();
	}
}
