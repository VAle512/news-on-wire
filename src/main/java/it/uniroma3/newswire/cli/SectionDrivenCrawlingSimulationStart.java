package it.uniroma3.newswire.cli;

import java.io.IOException;

import it.uniroma3.newswire.crawling.SDCrawlingDriver;

public class SectionDrivenCrawlingSimulationStart {
	public static void main(String[] args) throws IOException {
		SDCrawlingDriver.getInstance().run();
	}
}
