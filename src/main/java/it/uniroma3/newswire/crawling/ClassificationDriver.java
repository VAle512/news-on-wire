package it.uniroma3.newswire.crawling;

import org.apache.log4j.Logger;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import it.uniroma3.newswire.classification.RandomForestClassifier;
import it.uniroma3.newswire.properties.WebsiteConfiguration;
import scala.Tuple3;

/**
 * Questo è il driver che guida la fase di classificazione.
 * @author Luigi D'Onofrio
 *
 */
public class ClassificationDriver {
	private static Logger logger = Logger.getLogger(ClassificationDriver.class);
	private WebsiteConfiguration conf;
	
	/**
	 * Constructor.
	 * @param conf
	 */
	public ClassificationDriver(WebsiteConfiguration conf) {
		this.conf = conf;
	}
	
	/**
	 * Simula una classificazione ad un certo snapshot.
	 * @param snapshot
	 * @return
	 */
	public RandomForestModel simulateTrainAndClassify(int snapshot) {
		String websiteName = conf.getWebsite();
		try {
    		Tuple3<RandomForestModel, MulticlassMetrics, Double> result = null;
    		Tuple3<RandomForestModel, MulticlassMetrics, Double> bestResult = null;
    		
    		int maxIterations = 10;
    		int itCounter = 0;
    		
    		/* 10 iterazioni per essere sicuri. */
    		do {
    			/* Addestra */
    			result = RandomForestClassifier.train(websiteName, snapshot, conf.getFeatures().size(), true);
    			
    			if(bestResult == null)
    				bestResult = result;
    			else
    				if(result._3() < bestResult._3())
    					bestResult = result;
    			itCounter++;
    			logger.info("Classification iteration:" + itCounter);
    		} while(itCounter < maxIterations);
    		
    		/* Stampiamo qualche risultato */
    		RandomForestClassifier.printMetrics(bestResult._2());
    		System.out.println(bestResult._3());
    		
    		return result._1();
    		
		} catch (Exception e) {
			logger.error(websiteName + " -- " + e.getMessage());
			return null;
		}
	}

}
