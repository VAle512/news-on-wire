package it.uniroma3.streaming;

import static it.uniroma3.properties.PropertiesReader.GATHERER_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.GATHERER_TIME_TO_WAIT;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import it.uniroma3.properties.PropertiesReader;

public class Gatherer {
	private static Gatherer instance;
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final Logger logger = Logger.getLogger(Gatherer.class);
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(GATHERER_TIMEUNIT));
	private static final long TIME_TO_WAIT = TIME_UNIT.toMillis(Long.parseLong(propsReader.getProperty(GATHERER_TIME_TO_WAIT)));

	private Gatherer() {}

	public static void run() {
		new Thread(() -> runWGet()).start();
	}

	public static Gatherer getInstance() {
		return (instance==null) ? (instance=new Gatherer()) : instance;
	}

	private static void runWGet() {
		try {
			SimulatedAnnealing.run(0., 1);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
