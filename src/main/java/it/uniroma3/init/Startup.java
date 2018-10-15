package it.uniroma3.init;

import java.util.concurrent.TimeUnit;

import it.uniroma3.analysis.StabilityAnalysis;
import it.uniroma3.crawling.GraphController;
import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.spark.SparkLoader;

public class Startup {

	public static void main(String[] args) throws Exception {
		MySQLRepositoryDAO.getInstance().createStabilityTable();
		StabilityAnalysis.analyze();
		System.exit(0);
//		StabilityAnalysis.analyze();
//		System.exit(0);
//		MySQLRepositoryDAO.getInstance().createURLsTable();
//		MySQLRepositoryDAO.getInstance().createSequence();
//		MySQLRepositoryDAO.getInstance().createLinkOccourrencesTable();
		for (int i = 0; i < 14; ++i) {
			GraphController.crawlAndBuild();
//			//StabilityAnalysis.analyze();
//			System.out.println("Waiting 20 seconds.");
			System.out.println("Waiting...");
			Thread.sleep(TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES));
		}	
//		SparkLoader.getInstance().close();
			
	}

}
