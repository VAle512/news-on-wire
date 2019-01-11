package it.uniroma3.newswire.persistence;

import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_PERSISTENCE_BATCH_SIZE;
import static it.uniroma3.newswire.properties.PropertiesReader.CRAWLER_PERSISTENCE_THREAD_COUNT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.URLUtils;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import scala.Tuple4;
import scala.Tuple6;

/**
 * This class models a concurrent persister. 
 * Whenever a link is ready to be persisted, instead of doing this in the crawler we can delegate another guy to do so.
 * @author Luigi D'Onofrio
 *
 */
public class ConcurrentPersister extends Thread{
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private Logger logger = Logger.getLogger(ConcurrentPersister.class);
	
	private ProgressBar progressBar;

	private static final int BATCH_SIZE = Integer.parseInt(propsReader.getProperty(CRAWLER_PERSISTENCE_BATCH_SIZE));
	private static final int THREAD_COUNT = Integer.parseInt(propsReader.getProperty(CRAWLER_PERSISTENCE_THREAD_COUNT));
	private static final long WAITING_TIMEOUT_MILLIS = 150000;
	private Map<DAO, BlockingQueue<Tuple6<String, String, String, String, Integer, String>>> dao2queue;
	private ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
	

	/**
	 * Constructor.
	 */
	public ConcurrentPersister() {
		this.dao2queue = new HashMap<>();
	}
	
	/**
	 * Queues a tuple to be persisted in the future.
	 * @param tuple is the tuple we want to persist.
	 */
	public void addToQueue(Tuple6<String, String, String, String, Integer, String> tuple) {
		String absoluteURL = tuple._1();
		String dbName = URLUtils.domainOf(absoluteURL);
		
		DAO dao = DAOPool.getInstance().getDAO(dbName);
		
		/* If we don't have a queue for the specified database let's create it. */
		if(!this.dao2queue.containsKey(dao)) {
			BlockingQueue<Tuple6<String, String, String, String, Integer, String>> queueToPut = new LinkedBlockingQueue<>();			
			this.dao2queue.put(dao, queueToPut);
		}
		
		this.dao2queue.get(dao).add(tuple);
		
		/* Increment the progress bar max by 1 */
		/* Concurrency management needed but not now .*/
		this.progressBar.maxHint(this.progressBar.getMax() + 1);
		
		/* If there is at least one queue long enough, wake up the manager thread. */
		if(existsEligibleQueue())
			synchronized(this) {
				this.notify();
			}
				
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		try(ProgressBar pb = new ProgressBar("Persiting data", 0, ProgressBarStyle.UNICODE_BLOCK)) {
			/* Trickage */
			if(this.progressBar == null)
				this.progressBar = pb;
			
			while(true) {
				/* Check if there is at least one queue long enough .*/
				while(!existsEligibleQueue()) {
					synchronized(this) {
						try {
							/* Wait until there is a queue that is long enough. But to be sure, check every minute. */
							this.wait(WAITING_TIMEOUT_MILLIS);
						} catch (InterruptedException e) {
							logger.error(e.getMessage());
						}
					}
				}
				
				/* If there is, ask to another thread to do the job */
				executor.execute(() -> persistBatch());
			}
		}
	}
	
	/**
	 * Retrieves and returns the queue that has a size of at least BATCH_SIZE.
	 * @return the queue.
	 */
	private DAO retrieveEligibleQueue() {
		for(DAO dao: this.dao2queue.keySet())
			if(this.dao2queue.get(dao).size() >= BATCH_SIZE)
				return dao;	
		return null;
	}
	
	private boolean existsEligibleQueue() {
		return (this.retrieveEligibleQueue() != null);
	}
	
	/**
	 * This method does the job.
	 */
	public void persistBatch() {
		
		DAO dao = null;
		List<Tuple6<String, String, String, String, Integer, String>> toProcess = new ArrayList<>();

		synchronized(this.dao2queue) {
			dao = this.retrieveEligibleQueue();
			
			if(dao == null)
				return;
			
			/* We take a batch of tuples form the queue. */			
			for(int i = 0; i < BATCH_SIZE; ++i)
				if(!this.dao2queue.get(dao).isEmpty())
					if(!toProcess.add(this.dao2queue.get(dao).poll()))
						logger.error("Error polling value from queue of " + dao.getDatabaseName());
		}
		
		/* And we insert the batch onto the DB. */
		dao.insertLinkOccourrencesBatch(toProcess);
		
		/* Increase the counter of BATCH_SIZE once the task has completed. */
		this.progressBar.stepBy(BATCH_SIZE);
	}
}
