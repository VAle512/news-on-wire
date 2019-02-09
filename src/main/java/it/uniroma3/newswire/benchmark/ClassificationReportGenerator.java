package it.uniroma3.newswire.benchmark;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import it.uniroma3.newswire.classification.RandomForestClassifier;
import it.uniroma3.newswire.classification.features.Features;
import it.uniroma3.newswire.utils.EnvironmentVariables;
import it.uniroma3.newswire.utils.URLUtils;
import scala.Tuple3;

/**
 * Questa classe serve per generare un report riguardo un sito per il quale è stato fornito un golden per l'addestramento.
 * 
 * @author luigi
 *
 */
public class ClassificationReportGenerator {
	private Logger logger = Logger.getLogger(ClassificationReportGenerator.class);
	private String xlsFilePath;
	private String websiteRoot;
	private String databaseName;
	
	public ClassificationReportGenerator(String website) {
		this.websiteRoot = website;
		this.databaseName = URLUtils.getDatabaseNameOf(website);
		this.xlsFilePath = System.getenv(EnvironmentVariables.analysisResults) + File.separator + this.databaseName;
	}
	
	/*
	 * Developing only: 
	 */
	public void generateReport2ClassesOnly(int fromSnapsot, int toSnapshot) throws Exception {
		logger.info("Started creating report for: " + this.databaseName);
		logger.info("Storing: " + this.xlsFilePath);
		
		Workbook reportWorkbook = new XSSFWorkbook();
		
		/*
		 * Creiamo un foglio di lavoro.
		 */
		Sheet sheet = reportWorkbook.createSheet(this.websiteRoot);
		
		/*
		 * Creiamo un font per i titoli delel categorie.
		 */
		Font headerFont = reportWorkbook.createFont();
        headerFont.setBold(true);
        headerFont.setFontHeightInPoints((short) 14);
        headerFont.setColor(IndexedColors.WHITE.getIndex());
        
        /*
         * Creiamo uno stile per le celle dei titoli.
         */   
        CellStyle headerCellStyle = reportWorkbook.createCellStyle();
        headerCellStyle.setFont(headerFont);
        headerCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerCellStyle.setFillForegroundColor(IndexedColors.BLUE.getIndex());
  
        /*
         * Creiamo la riga di intestazione.
         */      
        Row headerRow = sheet.createRow(0);
        int rowCounter = headerRow.getRowNum() + 1;
        String columns[] = {"Precision", "Recall", "F-Measure"};
        
        for(int i = 1; i < columns.length * 2 + 1; ++i) {
        	int j = (i-1) % 3;
        	
        	Cell cell = headerRow.createCell(i);
        	cell.setCellValue(columns[j]);
        	cell.setCellStyle(headerCellStyle);
        }
        
        /*
         * Questa cella si trova immediatamente dopo all'ultima di intestazione.
         */
        int testErrorCell = columns.length * 2 + 1;
        Cell cell = headerRow.createCell(testErrorCell);
        cell.setCellValue("TestError");
        cell.setCellStyle(headerCellStyle);
        
        /*
         * Per ogni features che abbiamo installata, andiamo ad effettuare un addestramento (e valutazione) incrementali.
         */
        for(int i = 1; i <= 1; ++i) {
        	Row featuresRow = sheet.createRow(rowCounter);
        	featuresRow.createCell(0).setCellValue("Features: " + i);
  
        	rowCounter++;
        	
        	for(int s = fromSnapsot; s <= toSnapshot; s++) {
        		logger.info("Phase with " + i + " features @ snapshot " + s) ;
        		Tuple3<RandomForestModel, MulticlassMetrics, Double> result = null;
        		Tuple3<RandomForestModel, MulticlassMetrics, Double> bestResult = null;
        		
        		int maxIterations = 10;
        		int itCounter = 0;
        		
        		/*
        		 * Andiamo a provare più volte la K-Fold cross-validation ricercando una caratteristica particolare.
        		 * In caso eccediamo le maxIterations iteraziuoni prendiamo per buono il migliore.
        		 */
        		do {
        			result = RandomForestClassifier.train(this.websiteRoot, s, i);
        			
        			if(bestResult == null)
        				bestResult = result;
        			else
        				if(result._3() < bestResult._3())
        					bestResult = result;
        			itCounter++;
        			
        			logger.info("K-Fold Cross Validation iteration:" + itCounter);
        		} while(itCounter < maxIterations);
        		
        		RandomForestClassifier.printMetrics(bestResult._2());
        		System.out.println(bestResult._3());
        		
        		/*
        		 * Popoliamo le righe con i risultati appena ottenuti.
        		 */
        		Row newRow = sheet.createRow(rowCounter);
        		newRow.createCell(0).setCellValue(s);
        		
        		int featuresIndex = 1;
        		
        		for(int cls = 0; cls < 2; ++cls) {
        			newRow.createCell(featuresIndex).setCellValue(bestResult._2().precision(cls));
            		newRow.createCell(featuresIndex + 1).setCellValue(bestResult._2().recall(cls));
            		newRow.createCell(featuresIndex + 2).setCellValue(bestResult._2().fMeasure(cls));
            		
            		featuresIndex+=3;
        		}
        		
        		newRow.createCell(featuresIndex).setCellValue(bestResult._3());
        		
        		/*
        		 * Increase row counter.
        		 */
        		rowCounter++;
        	}
     
        }
        
        for(int i = 0; i < columns.length * 2 + 3; ++i)
        	sheet.autoSizeColumn(i);
        
        /*
         * Scriviamo su file.
         */
        FileOutputStream fileOut = new FileOutputStream(this.xlsFilePath + "_" + fromSnapsot + "_" + toSnapshot + ".xls");
        reportWorkbook.write(fileOut);
        
        fileOut.close();
        reportWorkbook.close();
	}
	
	public void generateReport(int fromSnapsot, int toSnapshot) throws Exception {
		logger.info("Started creating report for: " + this.databaseName);
		logger.info("Storing: " + this.xlsFilePath);
		
		Workbook reportWorkbook = new XSSFWorkbook();
		
		/*
		 * Creiamo un foglio di lavoro.
		 */
		Sheet sheet = reportWorkbook.createSheet(this.websiteRoot);
		
		/*
		 * Creiamo un font per i titoli delel categorie.
		 */
		Font headerFont = reportWorkbook.createFont();
        headerFont.setBold(true);
        headerFont.setFontHeightInPoints((short) 14);
        headerFont.setColor(IndexedColors.WHITE.getIndex());
        
        /*
         * Creiamo uno stile per le celle dei titoli.
         */   
        CellStyle headerCellStyle = reportWorkbook.createCellStyle();
        headerCellStyle.setFont(headerFont);
        headerCellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerCellStyle.setFillForegroundColor(IndexedColors.BLUE.getIndex());
  
        /*
         * Creiamo la riga di intestazione.
         */      
        Row headerRow = sheet.createRow(0);
        int rowCounter = headerRow.getRowNum() + 1;
        String columns[] = {"Precision", "Recall", "F-Measure"};
        
        for(int i = 1; i < columns.length * 3 + 1; ++i) {
        	int j = (i-1) % 3;
        	
        	Cell cell = headerRow.createCell(i);
        	cell.setCellValue(columns[j]);
        	cell.setCellStyle(headerCellStyle);
        }
        
        /*
         * Questa cella si trova immediatamente dopo all'ultima di intestazione.
         */
        int testErrorCell = columns.length * 3 + 1;
        Cell cell = headerRow.createCell(testErrorCell);
        cell.setCellValue("TestError");
        cell.setCellStyle(headerCellStyle);
        
        /*
         * Per ogni features che abbiamo installata, andiamo ad effettuare un addestramento (e valutazione) incrementali.
         */
        for(int i = 1; i <= Features.values().length; ++i) {
        	Row featuresRow = sheet.createRow(rowCounter);
        	featuresRow.createCell(0).setCellValue("Features: " + i);
  
        	rowCounter++;
        	
        	for(int s = fromSnapsot; s <= toSnapshot; s++) {
        		logger.info("Phase with " + i + " features @ snapshot " + s) ;
        		Tuple3<RandomForestModel, MulticlassMetrics, Double> result = null;
        		Tuple3<RandomForestModel, MulticlassMetrics, Double> bestResult = null;
        		
        		int maxIterations = 10;
        		int itCounter = 0;
        		
        		/*
        		 * Andiamo a provare più volte la K-Fold cross-validation ricercando una caratteristica particolare.
        		 * In caso eccediamo le maxIterations iteraziuoni prendiamo per buono il migliore.
        		 */
        		do {
        			result = RandomForestClassifier.train(this.websiteRoot, s, i);
        			
        			if(bestResult == null)
        				bestResult = result;
        			else
        				if(result._3() < bestResult._3())
        					bestResult = result;
        			itCounter++;
        			
        			logger.info("Iteration:" + itCounter);
        		} while(itCounter < maxIterations);
        		
        		RandomForestClassifier.printMetrics(bestResult._2());
        		System.out.println(bestResult._3());
        		
        		/*
        		 * Popoliamo le righe con i risultati appena ottenuti.
        		 */
        		Row newRow = sheet.createRow(rowCounter);
        		newRow.createCell(0).setCellValue(s);
        		
        		int featuresIndex = 1;
        		
        		for(int cls = 0; cls < 3; ++cls) {
        			newRow.createCell(featuresIndex).setCellValue(bestResult._2().precision(cls));
            		newRow.createCell(featuresIndex + 1).setCellValue(bestResult._2().recall(cls));
            		newRow.createCell(featuresIndex + 2).setCellValue(bestResult._2().fMeasure(cls));
            		
            		featuresIndex+=3;
        		}
        		
        		newRow.createCell(featuresIndex).setCellValue(bestResult._3());
        		
        		/*
        		 * Increase row counter.
        		 */
        		rowCounter++;
        	}
     
        }
        
        for(int i = 0; i < columns.length * 3 + 3; ++i)
        	sheet.autoSizeColumn(i);
        
        /*
         * Scriviamo su file.
         */
        FileOutputStream fileOut = new FileOutputStream(this.xlsFilePath + "_" + fromSnapsot + "_" + toSnapshot + ".xls");
        reportWorkbook.write(fileOut);
        
        fileOut.close();
        reportWorkbook.close();
	}
}
