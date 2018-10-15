package it.uniroma3.persistence;

import static it.uniroma3.properties.PropertiesReader.MONGODB_COUNTER_COLLECTION;
import static it.uniroma3.properties.PropertiesReader.MONGODB_COUNTER_ID;
import static it.uniroma3.properties.PropertiesReader.MONGODB_DB_NAME;
import static it.uniroma3.properties.PropertiesReader.MONGODB_HOST_ADDRESS;
import static it.uniroma3.properties.PropertiesReader.MONGODB_HOST_PORT;
import static it.uniroma3.properties.PropertiesReader.MONGODB_INPUT_COLLECTION;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import it.uniroma3.properties.PropertiesReader;

public class RepositoryDAO {
	private static RepositoryDAO instance;
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private Mongo connection = new MongoClient(propsReader.getProperty(MONGODB_HOST_ADDRESS), Integer.parseInt(propsReader.getProperty(MONGODB_HOST_PORT)));
	private final DB db;
	private DBCollection collection;

	@SuppressWarnings("deprecation")
	private RepositoryDAO() {
		this.db = connection.getDB(propsReader.getProperty(MONGODB_DB_NAME));
		this.collection = db.getCollection(propsReader.getProperty(MONGODB_INPUT_COLLECTION));
	}

	public void insertURLWithCheck(String url) {
		BasicDBObject urlObject = new BasicDBObject().append("url", url);
		DBObject urlFetched = this.collection.findOne(urlObject);
		
		if(urlFetched !=null) {
			Date actual = (Date)urlFetched.get("latestUpdate");		
			long diffInMillies = (new Date()).getTime() - actual.getTime();
			long newHours = TimeUnit.HOURS.convert(diffInMillies,TimeUnit.MILLISECONDS);
			DBObject update =  new BasicDBObject("$set", new BasicDBObject("latestUpdate", new Date()).append("hoursUptime", newHours));
			this.collection.update(urlFetched, update, true, false);
		} else {
			urlObject = urlObject.append("latestUpdate", new Date()).append("hoursUptime", 0);
			this.collection.insert(urlObject);
		}
	}
	
	public void insertURLWithSnapshot(String url, int snapshot) {
		BasicDBObject urlObject = new BasicDBObject().append("url", url);
		urlObject = urlObject.append("snapshot", snapshot);
		if(this.collection.findOne(urlObject)==null)
			this.collection.insert(urlObject);
	}
	
	public void insertURL(String url) {
		int snapshot = this.getNextSequence();
		BasicDBObject urlObject = new BasicDBObject().append("url", url);
		urlObject = urlObject.append("snapshot", snapshot);
		if(this.collection.findOne(urlObject)==null)
			this.collection.insert(urlObject);
	}

	public static RepositoryDAO getInstance() {
		return (instance==null) ? (instance = new RepositoryDAO()) : instance;
	}
	
	public void dropDB(String collectionName) {
		this.db.getCollection(collectionName).drop();
	}
	
	public Integer getNextSequence() {
		DBCollection sequenceCollection = this.db.getCollection(propsReader.getProperty(MONGODB_COUNTER_COLLECTION));
		DBObject sequenceObject = sequenceCollection.findOne(new BasicDBObject("_id", propsReader.getProperty(MONGODB_COUNTER_ID)));
		if(sequenceObject != null) {
			Integer counter = ((Double)sequenceObject.get("sequence_variable")).intValue();
			sequenceCollection.update(sequenceObject, new BasicDBObject("$inc", new BasicDBObject("sequence_variable", 1)));
			return counter;
		} else {
			return null;
		}
	}
	
	public Integer getCurrentSequence() {
		DBCollection sequenceCollection = this.db.getCollection(propsReader.getProperty(MONGODB_COUNTER_COLLECTION));
		DBObject sequenceObject = sequenceCollection.findOne(new BasicDBObject("_id", propsReader.getProperty(MONGODB_COUNTER_ID)));
		if(sequenceObject != null) {
			Integer counter = ((Double)sequenceObject.get("sequence_variable")).intValue();
			return counter;
		} else {
			return null;
		}
	}
	
	public List<String> getSnapshots() {
		DBCollection sequenceCollection = this.db.getCollection(propsReader.getProperty(MONGODB_COUNTER_COLLECTION));
		DBObject sequenceObject = sequenceCollection.findOne(new BasicDBObject("_id", propsReader.getProperty(MONGODB_COUNTER_ID)));
		List<String> snapshots = new ArrayList<>();
		int currentSequence = ((Double)sequenceObject.get("sequence_variable")).intValue();
		for(int i = 0; i < currentSequence; ++i)
			snapshots.add("S"+i);
		
		return snapshots;
	}
	
	public void insertLinkOccourrence(String pageFrom, String href, String absolute, String xpath, int snapshot) {
		DBCollection linksCollection = this.db.getCollection("LinkOccurrencesNew");
		DBObject linkObject = new BasicDBObject().append("page", pageFrom)
				                             .append("relative", href)
				                             .append("absolute", absolute)
				                             .append("xpath", xpath)
				                             .append("snapshot", snapshot);
		
		linksCollection.insert(linkObject);
	}
	
	public DBObject findByURL(String url) {
		return this.collection.findOne(new BasicDBObject("url", url));
	}
	
}
