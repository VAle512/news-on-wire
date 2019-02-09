package it.uniroma3.newswire.crawling;

/**
 * Questa classe si occupa di gestire l'intero processo per ogni sito web.
 * @author luigi
 *
 */
public class SystemDriver {
	private static SystemDriver instance;
	
	private SystemDriver() {}
	
	public static SystemDriver getInstance() {
		return (instance == null) ? instance = new SystemDriver() : instance;
	}
}
