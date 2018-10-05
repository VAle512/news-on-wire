package it.uniroma3.graphs;

import java.io.Serializable;

public class Node implements Serializable {
	private static final long serialVersionUID = -595383870933379548L;
	private String url;
	
	public Node(String url) {
		this.url = url;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public boolean equals(Object obj) {
		Node that = (Node) obj;
		return that.url.equals(this.url);
	}
	
	public String toString() {
		return this.url;
	}

}
