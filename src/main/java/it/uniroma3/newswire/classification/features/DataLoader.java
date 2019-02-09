package it.uniroma3.newswire.classification.features;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.date;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.id;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.relative;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.depth;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.file;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL_PLACEHOLDER;

import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import it.uniroma3.newswire.persistence.DAO;
import it.uniroma3.newswire.persistence.DAOPool;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.spark.SparkLoader;
import scala.Tuple2;

 /*
  * Questa classe si occupa di caricare in memoria i dati solamente una volta anzichè ogni volta e di cacharli.
  * Nel momento in cui non ci sono dati in meoria questi vengono caricati.
  * @author luigi
  *
  */
public class DataLoader {
	private static DataLoader instance;
	private static PropertiesReader propsReader = PropertiesReader.getInstance();
	private static Logger logger = Logger.getLogger(DataLoader.class);

	private static final String DB_NAME_PLACEHOLDER = propsReader.getProperty(MYSQL_DB_URL_PLACEHOLDER);
	private static final String LINK_OCCURRENCES = DAO.LINK_OCCURRENCES_TABLE;
	private static final String GOLDEN_CLASS = "golden_class";

	private int currentSnapshot;
	private SQLContext sqlContext;
	private JavaRDD<Document> loadedData;
	private JavaPairRDD<String, Integer> loadedGoldens;
	private boolean toResume = Boolean.parseBoolean(propsReader.getProperty("data.load.resumable"));
	
	
	/**
	 * Costruttore.
	 */
	@SuppressWarnings("deprecation")
	private DataLoader() {
		JavaSparkContext jsc = SparkLoader.getInstance().getContext();
		this.sqlContext = new SQLContext(jsc);
		this.currentSnapshot = 0;
		this.loadedData = jsc.emptyRDD();
	};
	
	/**
	 * @return Singleton.
	 */
	public static DataLoader getInstance() {
		return (instance == null) ? (instance = new DataLoader()) : instance;
	}
	
	/**
	 * Si occupa di caricare i dati da un db passato come parametro.
	 * @param dbName è il nome del db dal quale vogliamo caricare i dati.
	 * @return l'insieme dei dati.
	 */
	public JavaRDD<Document> loadData(String dbName) {
		String url = DAO.DB_URL.replace(DB_NAME_PLACEHOLDER, dbName);
		
		if(loadedData != null)
			return loadedData;
		else
			return this.loadedData = sqlContext.read()
											 	.format("jdbc")
											 	.option("url", url)
												.option("driver", DAO.JDBC_DRIVER)
												.option("dbtable", LINK_OCCURRENCES)
												.option("user", DAO.USER)
												.option("password", DAO.PASS)
												.load()
												.toJavaRDD()
												.map(row -> new Document().append(id.name(), 				(long)row.getLong(id.ordinal()))
																		  .append(link.name(), 				row.getString(link.ordinal()))
																		  .append(referringPage.name(), 	row.getString(referringPage.ordinal()))
															              .append(relative.name(), 		row.getString(relative.ordinal()))
															              .append(xpath.name(), 			row.getString(xpath.ordinal()))
															              .append(snapshot.name(), 		row.getInt(snapshot.ordinal()))
															              .append(date.name(), 				row.getTimestamp(date.ordinal()))
															              .append(depth.name(), 			row.getInt(depth.ordinal()))
															              .append(file.name(), 				row.getString(file.ordinal())))
												.cache();
	}
	
	/**
	 * Si occupa di caricare i dati da un db passato come parametro ma in maniera incrementale in modo da non appesantire il sistema.
	 * @param dbName è il nome del db dal quale vogliamo caricare i dati.
	 * @return l'insieme dei dati.
	 * @throws SQLException 
	 */
	public JavaRDD<Document> incrementalLoadData(String dbName, int toSnapshot, boolean isRange) throws SQLException {
		
		if(toResume) {
			if(toSnapshot == 2)
				this.currentSnapshot = 0;
			else if(toSnapshot > 2)
				this.currentSnapshot = toSnapshot - 1;
			
			this.toResume = false;
		}
		
		DAO dao = DAOPool.getInstance().getDAO(dbName);

		JavaSparkContext jsc = SparkLoader.getInstance().getContext();	
		/*
		 * Otteniamo dal DB la porzione di dati che ci occorre
		 */
		logger.info("loading data between: " + this.currentSnapshot + " - " + toSnapshot);
		
		List<Document> partialResults = dao.getLinkOccurrenciesBeforeSnapshot(this.currentSnapshot, toSnapshot, isRange);
		logger.info("Data retrieved from the Database for snapshot " + toSnapshot + ": " + partialResults.size());
		
		this.loadedData.unpersist();
		this.loadedData = jsc.parallelize(partialResults);
		this.loadedData.persist(StorageLevel.MEMORY_AND_DISK());
		logger.info("Current loaded data size: " + this.loadedData.count());
		
		return this.loadedData;									
	}
	
	/**
	 * Si occupa di caricare i dati dei golden samples.
	 * @param dbName è il nome del db del quale vogliamo caricare i samples.
	 * @return le coppie (url, classe) relative ai golden samples.
	 */
	public JavaPairRDD<String, Integer> loadGoldenData(String dbName) {
		String url = DAO.DB_URL.replace(DB_NAME_PLACEHOLDER, dbName);
		
		if(loadedGoldens != null)
			return loadedGoldens;
		else
			return this.loadedGoldens = sqlContext.read()
											 	.format("jdbc")
											 	.option("url", url)
												.option("driver", DAO.JDBC_DRIVER)
												.option("dbtable", GOLDEN_CLASS)
												.option("user", DAO.USER)
												.option("password", DAO.PASS)
												.load()
												.toJavaRDD()
												.mapToPair(row -> new Tuple2<>(row.getString(0), row.getInt(1)))
												.cache();

	}
	
	public void incrementSnapshotTo(int currentSnapshot) {
		this.currentSnapshot =  currentSnapshot;
	}
}
