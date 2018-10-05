package it.uniroma3.init;

import java.io.FileNotFoundException;
import java.io.IOException;

import it.uniroma3.batch.BatchAnalyzer;
import it.uniroma3.pagerank.GraphGenerator;
import it.uniroma3.streaming.Gatherer;
import it.uniroma3.streaming.kafka.KafkaInit;

public class Startup {

	public static void main(String[] args) throws FileNotFoundException, InterruptedException, IOException, ClassNotFoundException {
//		Gatherer.run();
//		KafkaInit.getInstance().init();
		
		GraphGenerator.generateGraph("www.ansa.it/sito/notizie/cronaca/cronaca.shtml");
	}

}
