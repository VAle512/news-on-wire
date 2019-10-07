package it.uniroma3.newswire.cli;

import it.uniroma3.newswire.benchmark.ClassificationReportGenerator;
import it.uniroma3.newswire.properties.PropertiesReader;

public class ReportStartup {
	public static void main(String[] args) throws Exception {
		int fromSnapshot = Integer.parseInt(PropertiesReader.getInstance().getProperty("fSnapshot"));
		int toSnapshot = Integer.parseInt(PropertiesReader.getInstance().getProperty("tSnapshot"));
		String siteName = PropertiesReader.getInstance().getProperty("analysisSiteName");
		new ClassificationReportGenerator(siteName).generateReport2ClassesOnly(fromSnapshot, toSnapshot);
	}
}
