package it.uniroma3.newswire.utils;

import static it.uniroma3.newswire.utils.EnvironmentVariables.envData;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Files;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import it.uniroma3.newswire.persistence.DAOPool;

public class PageSaver {
	private static final Logger logger = Logger.getLogger(PageSaver.class);

	public static String savePageOnFileSystem(Page page) {
		int currentSnapshot = DAOPool.getInstance().getDAO(URLUtils.getDatabaseNameOf(page.getWebURL().getURL())).getCurrentSequence();
		
		if (!(page.getParseData() instanceof HtmlParseData))
			return null;
		
		HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
		String url = URLUtils.canonicalize(page.getWebURL().getURL());
			
		if(page.getWebURL().getURL().endsWith("/"))
			url+="index.html";
		
		/* append snapshot counter to file name */
		url+=("." + currentSnapshot);
		
		/* persisting effective pages */
		String dataOutputPath = System.getenv(envData);
		String folderHierarchy = dataOutputPath + "/" + url.replace("http://", "");
		File f = new File(folderHierarchy);
		
		/* creates the folders hierarchy */
		f.getParentFile().mkdirs();
		
		try {
			f.createNewFile();
			Files.write(htmlParseData.getHtml(), f, StandardCharsets.UTF_8);
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		
		return f.getAbsolutePath();
	}
	
	public static String calculateFileName(String absolutePath) {
		int currentSnapshot = DAOPool.getInstance().getDAO(URLUtils.getDatabaseNameOf(absolutePath)).getCurrentSequence();
		String fileName = System.getenv(envData) + File.separator + absolutePath.replace("http://", "") + (absolutePath.endsWith("/") ? "index.html" : "") + "." + currentSnapshot;
		return fileName;
	}
}
