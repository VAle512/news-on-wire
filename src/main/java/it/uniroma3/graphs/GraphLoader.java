package it.uniroma3.graphs;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.spark_project.guava.io.Files;

public class GraphLoader {

	public GraphLoader() {
		// TODO Auto-generated constructor stub
	}

	public static Graph load() throws IOException {
		File file = new File("/home/luigidonofrio/Scrivania/generated.graph");
		Pattern pattern = Pattern.compile("\\(.*,\\s(.*)\\)\\s+\\(.*,\\s(.*)\\)");
		final Graph graph = new Graph();
		AtomicLong i = new AtomicLong(0);
		Files.readLines(file, StandardCharsets.UTF_8)
			 .stream()
			 .forEach(line -> {
				 	System.out.println("Loaded " + i.incrementAndGet() + " lines.");
				 	Matcher matcher = pattern.matcher(line);
				 if(matcher.find())
					 graph.addArch(new Node(matcher.group(1)), new Node(matcher.group(2)));
			 	});
		
		return graph;
	}

}
