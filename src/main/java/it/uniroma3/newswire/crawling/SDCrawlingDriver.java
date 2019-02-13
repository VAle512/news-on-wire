package it.uniroma3.newswire.crawling;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.parquet.Files;

import it.uniroma3.newswire.properties.WebsiteConfiguration;
import it.uniroma3.newswire.properties.WebsiteConfigurationReader;

/**
 * @author luigi
 *
 */
public class SDCrawlingDriver {
	private static SDCrawlingDriver instance;
	private static Logger logger = Logger.getLogger(SDCrawlingDriver.class);
	private List<EngineDriver> drivers;
	private ExecutorService threadPool = Executors.newCachedThreadPool();
	
	private SDCrawlingDriver() {
		try {
			this.drivers = initializeDrivers();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	public void run() throws IOException {
		
		this.drivers.forEach(d -> threadPool.execute(d));
	}
	
	public static SDCrawlingDriver getInstance() {
		return (instance == null) ? instance = new SDCrawlingDriver() : instance;
	}
		
	private List<EngineDriver> initializeDrivers() throws IOException {
		List<EngineDriver> drivers = new ArrayList<>();
		
		String configPath = System.getenv(envConfig);
		File seedFile = new File(configPath + "/" + "seeds");
		
		List<String> roots = Files.readAllLines(seedFile, StandardCharsets.UTF_8);
		WebsiteConfigurationReader confReader = WebsiteConfigurationReader.getInstance();
		
		for(String websiteRoot: roots) {
			WebsiteConfiguration websiteConf = confReader.getConfiguration(websiteRoot);
			
			CrawlingDriver crawlingDriver = new CrawlingDriver(new CrawlingModel(websiteRoot));
			FeatureExtractionDriver featureExtractionDriver = new FeatureExtractionDriver(websiteRoot, websiteConf.getFeatures());
			ClassificationDriver classificationDriver = new ClassificationDriver(websiteConf);
			
			drivers.add(new EngineDriver(crawlingDriver, featureExtractionDriver, classificationDriver, websiteConf));
		}
		
		return drivers;

	}
}
