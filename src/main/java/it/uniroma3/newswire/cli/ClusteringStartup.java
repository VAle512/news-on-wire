package it.uniroma3.newswire.cli;

import it.uniroma3.newswire.classification.Clusterer;
/*
 * Classe junky per sviluppo.
 */
public class ClusteringStartup {
	public static void main(String[] args) {
		Clusterer.calculate("nytimes_com", 40);
	}
}
