package it.uniroma3.newswire.properties;

import java.util.ArrayList;
import java.util.List;

import it.uniroma3.newswire.classification.features.Feature;

public class WebsiteConfiguration {
	private String website;
	private List<Feature> features;
	private long samplingFrequency;
	
	public WebsiteConfiguration(String website) {
		this.website = website;
		this.features = new ArrayList<>();
	}
	
	public void addFeature(Feature feature) {
		this.features.add(feature);
	}

	/**
	 * Restituisce il riferimento a samplingFrequency.
	 * @return the samplingFrequency
	 */
	public long getSamplingFrequency() {
		return samplingFrequency;
	}

	/**
	 * @param samplingFrequency the samplingFrequency to set
	 */
	public void setSamplingFrequency(long samplingFrequency) {
		this.samplingFrequency = samplingFrequency;
	}

	/**
	 * Restituisce il riferimento a features.
	 * @return the features
	 */
	public List<Feature> getFeatures() {
		return features;
	}

	/**
	 * Restituisce il riferimento a website.
	 * @return the website
	 */
	public String getWebsite() {
		return website;
	}
	
	
	
	
}
