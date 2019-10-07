package it.uniroma3.newswire.properties;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import it.uniroma3.newswire.classification.features.Feature;
import it.uniroma3.newswire.classification.features.PageHyperTextualContentDinamicity;
import it.uniroma3.newswire.classification.features.PageHyperTextualReferencesDinamicity;
import it.uniroma3.newswire.classification.features.PageHypertextualReferenceTrippingFactor;
import it.uniroma3.newswire.classification.features.Stability;
import it.uniroma3.newswire.classification.features.TextToBodyRatio;
import it.uniroma3.newswire.utils.URLUtils;

public class WebsiteConfigurationReader {
	public static final String FEATURES_LIST = "features";
	public static final String SAMPLING_FREQUENCY = "samplingFrequency";
	
	private static final String ENV_VAR_CONFIG = envConfig;
	
	private static Logger logger = Logger.getLogger(WebsiteConfigurationReader.class);
	private static WebsiteConfigurationReader instance;
	private Map<String, Properties> website2Properties;
	
	private WebsiteConfigurationReader() {
		this.website2Properties = new HashMap<>();
	}
	
	public static WebsiteConfigurationReader getInstance() {
		return (instance == null) ? instance = new WebsiteConfigurationReader(): instance;
	}
	
	private String getProperty(String website, String propertyName) {
		/* Se non è ancora stato caricato, carichiamolo. */
		if(!website2Properties.containsKey(website)) {
			Properties props = new Properties();
			String configPath = System.getenv(ENV_VAR_CONFIG);
			
			File propertiesFile = null;
			try {
				propertiesFile = new File(configPath + File.separator + URLUtils.getDatabaseNameOf(website) + ".config");
				if(!propertiesFile.exists())
					propertiesFile = new File(configPath + File.separator + "generic.config");
				
			}catch(Exception e) {
				/* Vuol dire che non è stato specificato alcun file per il sito */
				logger.error(e.getMessage());
			}finally {
				try {
					props.load(new FileReader(propertiesFile));
					website2Properties.put(website, props);
					logger.info("Configuration loaded: " + website);
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}	
		
		/* Otteniamo la property */
		return website2Properties.get(website).getProperty(propertyName);
		
	}
	
	public WebsiteConfiguration getConfiguration(String website) {
		WebsiteConfiguration conf = new WebsiteConfiguration(website);
		
		int samplingFrequency = Integer.parseInt(this.getProperty(website, "samplingFrequency"));
		String dbName = URLUtils.getDatabaseNameOf(website);
		
		List<Feature> features = new ArrayList<>();
		features.add(new Stability(dbName));
		features.add(new PageHyperTextualReferencesDinamicity(dbName));
		features.add(new PageHyperTextualContentDinamicity(dbName));
		features.add(new PageHypertextualReferenceTrippingFactor(dbName));
		
		List<String> featuresEnabled = Arrays.asList(this.getProperty(website, "features").split(",")).stream()
																										.map(x -> x.toLowerCase())
																										.collect(Collectors.toList());
		features = features.stream()
							.filter(feature -> featuresEnabled.contains(feature.getClass().getSimpleName().toLowerCase()))
							.collect(Collectors.toList());
		conf.setSamplingFrequency(samplingFrequency);
		features.forEach(f -> conf.addFeature(f));
		
		return conf;
	}
}
