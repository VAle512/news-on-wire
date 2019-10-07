package it.uniroma3.newswire.cli;

import java.io.IOException;

import it.uniroma3.newswire.section_driven_crawling.Engine;

public class SectionDrivenCrawlingSimulationStart {
	public static void main(String[] args) throws IOException {
		Engine.getInstance().run();
	}
}
