package it.uniroma3.streaming;

import static it.uniroma3.properties.PropertiesReader.GATHERER_TIMEUNIT;
import static it.uniroma3.properties.PropertiesReader.GATHERER_TIME_TO_WAIT;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.spark_project.guava.collect.Lists;

import it.uniroma3.batch.BatchAnalyzer;
import it.uniroma3.properties.PropertiesReader;
import it.uniroma3.streaming.kafka.StreamAnalyzer;
import it.uniroma3.streaming.utils.WGetForURLs;

public class SimulatedAnnealing {
	private static final Logger logger = Logger.getLogger(SimulatedAnnealing.class);
	private static double T;
	private static int V_C;
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	private static final int NEIGHBOURHOOD_FACTOR = 3;
	private static final TimeUnit TIME_UNIT = TimeUnit.valueOf(propsReader.getProperty(GATHERER_TIMEUNIT));
	private static final long TIME_TO_WAIT = TIME_UNIT.toMillis(Long.parseLong(propsReader.getProperty(GATHERER_TIME_TO_WAIT)));

	private static boolean HALTING_CONDITION;
	
	public static void run(double Temp, int startingDepth) throws ClassNotFoundException, 
											   IOException, 
											   InterruptedException {
		logger.info("Starting simulated annealing...");
		V_C = 15;
		T = Temp;
		HALTING_CONDITION = (T >= V_C);
		
		List<Integer> depths = Lists.newArrayListWithCapacity(V_C);
		depths.stream()
			  .map(x -> depths.indexOf(x))
			  .collect(Collectors.toList());
		
		/* Pick Random depth */
		Random randomPick = new Random();
		//int currentDepth = randomPick.nextInt(V_C);
		int currentDepth = startingDepth;
		/* Evaluate Vc */
		double currentEval = evaluateDepth(currentDepth);
		logger.info("Goodness for depth " + currentDepth + ": " + currentEval);
		/* Let's wait a bit */
		logger.info("Idling for " + TIME_UNIT.convert(TIME_TO_WAIT, TimeUnit.MILLISECONDS) + " " + TIME_UNIT.toString());
		Thread.sleep(TIME_TO_WAIT);
		
		while(!HALTING_CONDITION) {
				final int exclude = currentDepth;
				/*pick a number in the neighbourhood of Vc */ 
				int lowerBound = currentDepth - NEIGHBOURHOOD_FACTOR;
				int upperBound = currentDepth + NEIGHBOURHOOD_FACTOR;
				logger.info("Selecting a number between " + lowerBound + " and " + upperBound + " except " + exclude);
				int newDepth = (new Random()).ints(lowerBound, upperBound)
											 .filter(x -> x != exclude).findAny().getAsInt();
				
				logger.info("Picked neighbor: " + newDepth);
				double newEval = evaluateDepth(newDepth);
				logger.info("Goodness for depth " + newDepth + ": " + newEval);
				double probability = Math.exp((newEval - currentEval) * T);
				double random = 0.;
				if (newEval > currentEval)
					currentDepth = newDepth;	
				else if ((random = (new Random()).nextDouble()) < probability)
						currentDepth = newDepth;
				
				logger.info("T: " + T); 
				logger.info("Current Depth: " + currentDepth);
				if(random!= 0.)
					logger.info("Decision taken due to: [R:" + random + ", P:" + probability+"]");
				T = T + 1;
				
				/* Let's wait a bit */
				logger.info("Idling for " + TIME_UNIT.convert(TIME_TO_WAIT, TimeUnit.MILLISECONDS) + " " + TIME_UNIT.toString());
				Thread.sleep(TIME_TO_WAIT);
		}
		
	}

	
	private static Double evaluateDepth(int depth) throws ClassNotFoundException, 
														IOException, 
														InterruptedException {
		StreamAnalyzer.getInstance().resetCoverage();
		logger.info("Starting URLs Gathering with depth " + depth);
			WGetForURLs.explore(depth);
		logger.info("URLs Gathering: complete.");
		BatchAnalyzer.runOneInstance();
		return StreamAnalyzer.getInstance().getCoverage();
	}

}
