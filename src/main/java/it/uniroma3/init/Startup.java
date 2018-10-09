package it.uniroma3.init;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.spark_project.guava.io.Files;

import it.uniroma3.classifier.PagesClassifier;
import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.GraphLoader;

public class Startup {

	public static void main(String[] args) throws Exception {
		Graph graph = GraphLoader.load();
		PagesClassifier.classify(graph);
		graph.getGraphNodes().forEach(node -> {
			try {
				Files.append(node + System.lineSeparator(), new File("/home/Scrivania/classificationResults"), StandardCharsets.UTF_8);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

}
