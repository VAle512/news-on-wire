package it.uniroma3.newswire.sectiondrivencrawling.conditions;

/**
 * Interfaccia per la specifica di condizioni di terminazione. E' un Work in progress. 
 * Bisognerebbe modellare uno stato del crawling per permettere una specifica veramente modulare.
 * @author Luigi D'Onofrio
 *
 */
public interface PluggablePhaseOneTerminationCondition {
	/**
	 * @return true se la condizione è rispettata, false altrimenti.
	 */
	public boolean isTerminated();
}
