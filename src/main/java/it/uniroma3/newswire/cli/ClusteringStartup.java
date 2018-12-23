package it.uniroma3.newswire.cli;

import it.uniroma3.newswire.classification.ThreeMeans;

public class ClusteringStartup {
	public static void main(String[] args) {
		ThreeMeans.calculate("bbc_com", 5);
	}
}
