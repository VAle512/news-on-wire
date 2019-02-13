package it.uniroma3.newswire.crawling;

/**
 * Semplice condizione di terminazione basata su una threshold sullo snapshot. Usata tipicamente nella fase 1 del section dirven crawling.
 * @author luigi
 *
 */
public class SnapshotThreshold implements PluggablePhaseOneTerminationCondition{
	private int currentSnapshot;
	private int threshold;
	
	/**
	 * Constructor.
	 * @param threshold
	 */
	public SnapshotThreshold(int threshold) {
		this.threshold = threshold;
	}
	
	/* (non-Javadoc)
	 * @see it.uniroma3.newswire.crawling.PluggablePhaseOneTerminationCondition#isTerminated()
	 */
	@Override
	public boolean isTerminated() {
		return this.currentSnapshot <= this.threshold;
	}


	/**
	 * Restituisce il riferimento a currentSnapshot.
	 * @return the currentSnapshot
	 */
	public int getCurrentSnapshot() {
		return currentSnapshot;
	}


	/**
	 * @param currentSnapshot the currentSnapshot to set
	 */
	public void updateCurrentSnapshot(int currentSnapshot) {
		this.currentSnapshot = currentSnapshot;
	}

}
