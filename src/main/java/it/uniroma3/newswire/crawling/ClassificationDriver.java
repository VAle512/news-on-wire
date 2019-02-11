package it.uniroma3.newswire.crawling;

import org.apache.log4j.Logger;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import it.uniroma3.newswire.classification.RandomForestClassifier;
import it.uniroma3.newswire.properties.WebsiteConfiguration;

public class ClassificationDriver {
	private static Logger logger = Logger.getLogger(ClassificationDriver.class);
	private WebsiteConfiguration conf;
	
	public ClassificationDriver(WebsiteConfiguration conf) {
		this.conf = conf;
	}
	public RandomForestModel trainAndClassify(int snapshot) {
		String websiteName = conf.getWebsite();
		try {
			return RandomForestClassifier.train(websiteName, snapshot, conf.getFeatures().size(), false)._1();
		} catch (Exception e) {
			logger.error(websiteName + " -- " + e.getMessage());
			return null;
		}
	}
	
	public RandomForestModel simulateTrainAndClassify(int snapshot) {
		String websiteName = conf.getWebsite();
		try {
			return RandomForestClassifier.train(websiteName, snapshot, conf.getFeatures().size(), true)._1();
		} catch (Exception e) {
			logger.error(websiteName + " -- " + e.getMessage());
			return null;
		}
	}

}
