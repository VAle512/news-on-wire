package it.uniroma3.graphs;

import java.io.Serializable;

/**
 * This class models a node of a {@Graph}. 
 * Every node has its own name and {@link PageType}.
 * @author Luigi D'Onofrio
 */
public class Node implements Serializable {
	private static final long serialVersionUID = -595383870933379548L;
	private String name;
	/* Before classification every node has a genric type */
	private PageType nodeType = PageType.OTHER;

	/**
	 * Constructor.
	 * @param name is the name we want for this {@link Node}.
	 */
	public Node(String name) {
		this.name = name;
	}

	/**
	 * @return a {@link String} representation of the name of this {@link Node}.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * @return the {@link PageType} of this {@link Node}.
	 */
	public PageType getNodeType() {
		return nodeType;
	}
	
	/**
	 * This method sets the {@link PageType} of this {@link Node}.
	 */
	public void setNodeType(PageType nodeType) {
		this.nodeType = nodeType;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		Node that = (Node) obj;
		return that.name.equals(this.name);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "(" + this.nodeType.toString() + ", " + this.name + ")";
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return this.name.hashCode();
	}

}
