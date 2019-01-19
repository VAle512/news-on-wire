package it.uniroma3.newswire.cli;

import static it.uniroma3.newswire.crawling.CrawlingDriver.crawl;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIMEUNIT;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_TIME_TO_WAIT;
import static it.uniroma3.newswire.utils.EnvironmentVariables.envConfig;
import static it.uniroma3.newswire.utils.EnvironmentVariables.goldens;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import it.uniroma3.newswire.benchmark.ClassificationReportGenerator;
import it.uniroma3.newswire.classification.KFoldCrossValidation;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;

public class Init {
	private static final String FORCE_CRAWLING_PROPERTY = "force-crawling"; 
	private static final String USE_CLI_PROPERTY = "cli";
	
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private static Logger logger = Logger.getLogger(CLI.class);
	private static int timeToWait = Integer.parseInt(propsReader.getProperty(CRAWLER_TIME_TO_WAIT));

	private static final TimeUnit TIME_TO_WAIT_TIMEUNIT = TimeUnit.valueOf(propsReader.getProperty(CRAWLER_TIMEUNIT));
	
	public static void main(String[] args) throws Exception {
		/* Check for VM arguments */
		boolean forceCrawlOnly = Boolean.parseBoolean(System.getProperty(FORCE_CRAWLING_PROPERTY));
		boolean useCLI = Boolean.parseBoolean(System.getProperty(USE_CLI_PROPERTY));
		
//		if(forceCrawlOnly)
//			doCrawl();
//			Workbook workbook = new XSSFWorkbook();
//			
//			Sheet sheet = workbook.createSheet("Data");
//			
//			Font headerFont = workbook.createFont();
//			headerFont.setBold(true);
//			headerFont.setFontHeightInPoints((short)14);
//			
//			CellStyle headerCellStyle = workbook.createCellStyle();
//	        headerCellStyle.setFont(headerFont);
//	        
//	        Row headerRow = sheet.createRow(0);
//	        
//	        for(int i = 0; i < 4; i++) {
//	            Cell cell = headerRow.createCell(i);
//	            cell.setCellValue("ciao");
//	            cell.setCellStyle(headerCellStyle);
//	        }
//	        
//	        FileOutputStream fileOut = new FileOutputStream("poi-generated-file.xlsx");
//	        workbook.write(fileOut);
//	        fileOut.close();
//			CSVGenerator gen = new CSVGenerator();
//			gen.generateCSV("www.nytimes.com", Integer.parseInt(PropertiesReader.getInstance().getProperty("classification.snapshots")));
//			KFoldCrossValidation.run("/home/luigi/git/newswire/csv/nytimes_com_18_training.csv", 10);
//			
//			TextToBodyRatio ttbr = new TextToBodyRatio("nytimes_com", "www.nytimes.com");
//			ttbr.calculate(true, 3);
			new ClassificationReportGenerator("www.nytimes.com", 24).generateReport();
//			PageHypertextualReferenceTrippingFactor trip = new PageHypertextualReferenceTrippingFactor("nytimes_com");
//			trip.calculate(true, 20);
			
//			Stability trip = new Stability("bbc_com");
//			trip.calculate(true, 5);
			
//		else if (useCLI)
//		showCLI();
		
//		(new Engine()).start("http://www.ansa.it", 0);
		
		
	}
	
	@SuppressWarnings("static-access")
	//TODO: Codice duplicato not good.
	private static void doCrawl() throws Exception {
		logger.info("Crawling started...");
		boolean resetAll = true;
		/* Personal protection. */
//		boolean resetAll = Boolean.parseBoolean(propsReader.getProperty(MYSQL_RESET_ALL));
		/* Never erase anything resume everytime */
		
		//checkGoldens();
		
		init(resetAll);
		
		while(true) {
			//crawl from seed file
			crawl();
			logger.info("Waiting for " + timeToWait + " " + TIME_TO_WAIT_TIMEUNIT.toString().toLowerCase());
			Thread.currentThread().sleep(TIME_TO_WAIT_TIMEUNIT.toMillis(timeToWait));
		}
	}
	
	private static void checkGoldens() {
		File goldenDir = new File(System.getenv(goldens));
		List<String> databases = DAOPool.getInstance().getDatabasesDAOsByName();

		for(String database : databases)
			if(Arrays.asList(goldenDir.list()).contains(database + ".csv"))
				logger.info(database + ": golden loaded successfully.");
			else
				logger.error(database + ": golden not found.");
	}

	protected static void init(boolean resetAll) throws IOException {
		String configPath = System.getenv(envConfig);
		File seedFile = new File(configPath + File.separator + "seeds");
		
		//TODO: Rivedere responsabilità e anche praticità
		//TODO: Aggiungere l'eliminazione anche dei file, controllare responsabilità.
		if(resetAll)
			Files.readLines(seedFile, StandardCharsets.UTF_8)
				 .stream()
				 .forEach(seed -> DAOPool.getInstance().getDAO(URLUtils.getDatabaseNameOf(seed)).resetData());	
			
		DAOPool.getInstance().loadAllDAOs();
		
	}

}
