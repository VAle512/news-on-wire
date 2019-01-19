package it.uniroma3.newswire.persistence;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.date;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.depth;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.file;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.relative;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL_PLACEHOLDER;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_JDBC_DRIVER;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_LINK_COLLECTIONS_TABLE_NAME;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_LINK_OCCURRENCES_TABLE_NAME;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_PASS;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_SEQUENCE_ID;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_SEQUENCE_TABLE_NAME;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_USER;
import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Level.INFO;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.neo4j.driver.internal.util.Iterables;

import com.google.common.collect.Lists;

import it.uniroma3.newswire.benchmark.BenchmarkResult;
import it.uniroma3.newswire.classification.features.PageHyperTextualReferencesDinamicity;
import it.uniroma3.newswire.classification.features.HyperTextualContentDinamycityPlusStability;
import it.uniroma3.newswire.classification.features.Stability;
import it.uniroma3.newswire.persistence.schemas.LinkOccourrences;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.xpath.XPath;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple6;

/**
 * This class is an interface to the MySQL relational database in which are stored all the informations
 * used in this project.
 * @author Luigi D'Onofrio
 *
 */
//TODO: replace all the static queries using enum values.
//TODO: Check for unused methods.
public class DAO implements Serializable{	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2689974262603276962L;
	private static final Logger logger = Logger.getLogger(DAO.class);
	private static final PropertiesReader propsReader = PropertiesReader.getInstance();
	
	/**
	 * The JDBC driver identifier.
	 */
	public static final String JDBC_DRIVER = propsReader.getProperty(MYSQL_JDBC_DRIVER);
	/**
	 * The Database name we want to connect to.
	 */
	public static final String DB_URL = propsReader.getProperty(MYSQL_DB_URL);
	/**
	 * Username provided for the authentication.
	 */
	public static final String USER = propsReader.getProperty(MYSQL_USER);
	/**
	 * Password provided for the authentication.
	 */
	public static final String PASS = propsReader.getProperty(MYSQL_PASS);
	/**
	 * The name of the table used to store the Link Occurrences.
	 */
	public static final String LINK_OCCURRENCES_TABLE = propsReader.getProperty(MYSQL_LINK_OCCURRENCES_TABLE_NAME);	
	/**
	 * The name of the table used to store the Link Occurrences.
	 */
	public static final String LINK_COLLECTIONS_TABLE = propsReader.getProperty(MYSQL_LINK_COLLECTIONS_TABLE_NAME);
	/**
	 * The name of the sequence table.
	 */
	private static final String SEQUENCE_TABLE_NAME = propsReader.getProperty(MYSQL_SEQUENCE_TABLE_NAME);
	/**
	 * The name of the sequence we want to use.
	 */
	private static final String SEQUENCE_ID = propsReader.getProperty(MYSQL_SEQUENCE_ID);
	/**
	 * Placeholder to be replaced by the real database name.
	 */
	private static final String DB_NAME_PLACEHOLDER = propsReader.getProperty(MYSQL_DB_URL_PLACEHOLDER);
    /**
     * The Connection pool.
     */
    private BasicDataSource dataSource;
    
	/**
	 * The database name.
	 */
	private String dbName;
	
	/**
	 * Constructor.
	 */
	private DAO(String dbName) {
		try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			log(ERROR, "Driver class not found: " + JDBC_DRIVER);
		}
		this.dataSource = new BasicDataSource();
		this.dataSource.setDriverClassName(JDBC_DRIVER);
		String url = DB_URL.replace(DB_NAME_PLACEHOLDER, dbName);
        this.dataSource.setUrl(url);
        this.dataSource.setUsername(USER);
        this.dataSource.setPassword(PASS);
        this.dataSource.setMaxOpenPreparedStatements(16);
        this.dataSource.setInitialSize(24);
        this.dataSource.setMaxTotal(-1);
        
        this.dbName = dbName;
        
	}
	
	/**
	 * Creates (and removes, if already existing) a new table used to store link occurrences.
	 */
	public void createLinkOccourrencesTable() {
		log(INFO,"Creating Table [" + LINK_OCCURRENCES_TABLE + "]...");
		
		Statement statement = null;
		Connection connection = null;
		try {
			connection = this.getConnection();
			statement = connection.createStatement();
			
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE IF EXISTS " + LINK_OCCURRENCES_TABLE;
			try {
				statement.execute(dropTableQuery);
				log(INFO, "Old Table dropped [" + LINK_OCCURRENCES_TABLE + "].");
			} catch(SQLException e) {
				log(ERROR, e.getMessage());
			}
			
			/* ...and then (re)create it. */
			//XXX: Aggiungere Depth e Pathinse
			String createTableQuery = "CREATE TABLE " + LINK_OCCURRENCES_TABLE + " (id bigint AUTO_INCREMENT, "
																			   +   "link varchar(2083) NOT NULL, "
																			   +   "referringPage varchar(2083) NOT NULL, "
																			   +   "relativeLink varchar(2083), "
																			   +   "xpath varchar(2083), "
																			   +   "snapshot int NOT NULL, "
																			   +   "date Timestamp, "
																			   +   "depth int NOT NULL,"
																			   +   "file varchar(2083),"
																			   +   "PRIMARY KEY(id))";
			statement.execute(createTableQuery);
			
			log(INFO,"Table created [" + LINK_OCCURRENCES_TABLE + "].");

		} catch (SQLException e) {
			log(ERROR, e.getMessage());
		}finally {
			clearResources(connection, statement, null, null);
		}
	}
	
	public void createLinkCollectionsTable() {
		log(INFO,"Creating Table [" + LINK_COLLECTIONS_TABLE + "]...");

		Statement statement = null;
		Connection connection =  null;
		
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE IF EXISTS " + LINK_COLLECTIONS_TABLE;
			try {
				statement.execute(dropTableQuery);
				log(INFO,"Old Table dropped [" + LINK_COLLECTIONS_TABLE + "].");
			} catch(SQLException e) {}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + LINK_COLLECTIONS_TABLE + "(id bigint NOT NULL, "
																	  			+ "collection varchar(255) NOT NULL,"
																	  			+ "FOREIGN KEY (id) REFERENCES LinkOccourrences(id))";
			statement.execute(createTableQuery);
			log(INFO,"Table created [" + LINK_COLLECTIONS_TABLE + "].");

		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		}finally {
			clearResources(connection, statement, null, null);
		}
	}
	
	public void createAnalysisTable(String tableName) {
		log(INFO,"Creating Table [" + tableName + "]...");

		Statement statement = null;
		Connection connection =  null;
		
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE IF EXISTS " + tableName;
			try {
				statement.execute(dropTableQuery);
				log(INFO,"Old Table dropped [" + tableName + "].");
			} catch(SQLException e) {}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + tableName + "(url varchar(2083) NOT NULL, "
																	  + "score double NOT NULL)";
			statement.execute(createTableQuery);
			log(INFO,"Table created [" + tableName + "].");

		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		}finally {
			clearResources(connection, statement, null, null);
		}
	}
	
	public void createBenchmarkTable(String benchmarkName) {
		log(INFO,"Creating Table [" + benchmarkName + "]...");

		Statement statement = null;
		Connection connection =  null;
		
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE IF EXISTS " + benchmarkName;
			try {
				statement.execute(dropTableQuery);
				log(INFO,"Old Table dropped [" + benchmarkName + "].");
			} catch(SQLException e) {}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + benchmarkName + "(snapshot int(11) NOT NULL, "
																	  +  "precis double NOT NULL, "
																	  +  "recall double NOT NULL, "
																	  +  "f1 double NOT NULL, "
																	  +  "threshold double NOT NULL)";
			statement.execute(createTableQuery);
			log(INFO,"Table created [" + benchmarkName + "].");

		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		}finally {
			clearResources(connection, statement, null, null);
		}
	}
	
	/**
	 * Creates (or resets, if already existing) a new sequence used to track snapshots.
	 */
	public void createSequence() {
		log(INFO,"Creating Sequence [" + SEQUENCE_TABLE_NAME + "]...");

		Statement statement = null;
		Connection connection = null;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* Try to create the table... */
			String createTableQuery = " CREATE TABLE " + SEQUENCE_TABLE_NAME + " (id varchar(255) PRIMARY KEY NOT NULL, "
																			   + "sequence_value int)";
			statement.execute(createTableQuery);
			
			/* Insert the first value... */
			String insertFirstValue = "INSERT INTO " + SEQUENCE_TABLE_NAME + " VALUES ('" + SEQUENCE_ID + "', 0)";
			statement.executeUpdate(insertFirstValue);
			log(INFO,"Sequence created [" + SEQUENCE_TABLE_NAME + "].");

		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		}finally {
			try {
				/* Dirty usage of exceptions: update the value in every case. */
				statement.executeUpdate("UPDATE " + SEQUENCE_TABLE_NAME + " SET sequence_value = 0 WHERE id = '" + SEQUENCE_ID + "'");
				
				/* Releasing resources. */
				clearResources(connection, statement, null, null);
				
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
		}
	}
	
	/**
	 * Increments the sequence by one.
	 */
	public void incrementSequence() {
		Statement statement = null;
		Connection connection = null;
		
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* Try to update the sequence... */
			String updateSequenceQuery = "UPDATE " + SEQUENCE_TABLE_NAME + " SET sequence_value = sequence_value + 1 WHERE id = '" + SEQUENCE_ID + "'";
			statement.executeUpdate(updateSequenceQuery);
			log(INFO,"Sequence incremented [" + SEQUENCE_TABLE_NAME +"]");
			
		} catch (SQLException e) {
			log(ERROR,"Sequence [" + SEQUENCE_TABLE_NAME + "] not created.");
		}finally {
			clearResources(connection, statement, null, null);
		}
	}
	
	/**
	 * @return the current sequence value. It returns -1 if something goes wrong.
	 */
	public int getCurrentSequence() {
		Statement statement = null;
		ResultSet result = null;
		Connection connection = null;
		int snapshot = 0;
		
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* Try to get the current sequence value... */
			String getCurrentSequenceValueQuery = "SELECT sequence_value FROM " +  SEQUENCE_TABLE_NAME + " WHERE id = '"+ SEQUENCE_ID + "'";
			result = statement.executeQuery(getCurrentSequenceValueQuery);

			if(result.next()) 
				snapshot = result.getInt(1);  

		} catch (SQLException e) {
			log(ERROR,e.getMessage());
			snapshot = -1;
		}finally {
			clearResources(connection, statement, null, result);
		}
		return snapshot;

	}
	
	public void insertAnalysisResult(String tableName, Iterator<Tuple2<String, Double>> docs) {
		
		PreparedStatement statement = null;
		Connection connection = getConnection();
		try {
			String  insertResultPrepared = "INSERT INTO " + tableName + "(url, score) VALUES (?,?)";
			statement = connection.prepareStatement(insertResultPrepared);
		}catch(SQLException e) {
			log(ERROR, "Preparing statment.");
		}
			
		
		PreparedStatement finalStat = statement;
		AtomicInteger count = new AtomicInteger(0);
		
		docs.forEachRemaining(doc -> {
						try {
							finalStat.setString(1, doc._1);
							finalStat.setDouble(2, doc._2);
							finalStat.executeUpdate();
							count.incrementAndGet();
						}catch(SQLException e3) {
							log(ERROR,e3.getMessage());
						}
						
				});		
		
		clearResources(connection, statement, null, null);
		log(INFO, "Persisted " + count + " objects");

	}
	
	//TODO: Refactor usando BenchmarkResult
	public void insertBenchmark(String benchmarkName, int snapshot, double precision, double recall, double f1, double threshold) {
		/* Check if some value is NaN */
		if(Double.isNaN(precision))
			precision = 0.;
		if(Double.isNaN(recall))
			recall = 0.;
		if(Double.isNaN(f1))
			f1 = 0.;
		if(Double.isNaN(threshold))
			threshold=0.;
		
		Statement statement = null;
		Connection connection = this.getConnection();
		try {
			
			String  insertQuery = "INSERT INTO " + benchmarkName + "(snapshot, precis, recall, f1, threshold) VALUES (" + snapshot + ", " + precision + ", " + recall + ", " + f1 + ", " + threshold + ")";
			statement = connection.createStatement();
				
			try {
				statement.executeUpdate(insertQuery);
			} catch (SQLException e1) {
				log(ERROR,e1.getMessage());			
			}finally {
				clearResources(connection, statement, null, null);
			}
		}catch(SQLException e3) {
			log(ERROR,e3.getMessage());
		
		}
	}
	
	public Double getMaxBenchmarkScore(String benchmarkName) {
		Statement statement = null;
		ResultSet result = null;
		Connection connection = getConnection();
		try {
			
			String  insertResultPrepared = "SELECT max(score) FROM " + benchmarkName;
			statement = connection.createStatement();
			result = statement.executeQuery(insertResultPrepared);	
			
			if(result.next())
				return result.getDouble(1);
			
			} catch (SQLException e1) {
				log(ERROR,e1.getMessage());
				return null;
			} finally {
				clearResources(connection, statement, null, result);
			}
		
		return null;
	}
	
	private void clearResources(Connection connection, Statement statement, PreparedStatement prepStatement, ResultSet resultSet) {
		try {
			/* Releasing resources. */
			if(statement != null)
				statement.close();
			
			if(connection != null)
				connection.close();
				
			if(resultSet != null)
				resultSet.close();
				
			if(prepStatement != null)
				prepStatement.close();
			
		}catch(SQLException e2) {
			log(ERROR,e2.getMessage());
		}
	}
	
	
	/**
	 * Inserts a link occurrence into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param linkToInsert is the link we want to add the occurrence for (absolute URL).
	 * @param referringPageToInsert is the page we found this link in.
	 * @param relativeToInsert is the relative URL of the link we are adding the occurrence for.
	 * @param xpathToInsert is the position where we found this link.
	 */
	//TODO: Cambiare in bigInt l'autoincrement e poi pushare.
	//XXX: Aggiungere Depth e Path
	public void insertLinkOccourrence(Connection connection, String linkToInsert, String referringPageToInsert, String relativeToInsert, String xpathToInsert, int depth, String stored) {
		Statement statement = null;
		PreparedStatement pstat = null;
		
		try {
			if(linkToInsert.equals(referringPageToInsert))
				throw new SQLException("Found self reference.");

			/* Retrieve the current snapshot counter. */
			int currentSnapshot = this.getCurrentSequence();
			
			boolean alreadyThere = connection.createStatement().executeQuery("SELECT * FROM " + LINK_OCCURRENCES_TABLE 
																		+ 	" WHERE link='" + linkToInsert +"' AND"
																		+ 	" referringPage='" + referringPageToInsert + "' AND"
																		+ 	" relativeLink='" + relativeToInsert + "' AND"
																		+ 	" xpath='" + xpathToInsert + "' AND"
																		+ 	" snapshot=" + currentSnapshot).next();
			
			/* If xpath's null it's useless */
			if(alreadyThere  || xpathToInsert==null) 
				throw new SQLException();	
			
			
		    Timestamp sqlDate = new Timestamp(new java.util.Date().getTime());
		    
		    String insertLinkOccurrenceQuery = "INSERT INTO " + LINK_OCCURRENCES_TABLE + "(link, referringPage, relativeLink, xpath, snapshot, depth, file, date) values(?,?,?,?,?,?,?,?)";	    
		    
		    pstat = connection.prepareStatement(insertLinkOccurrenceQuery);
		    
		    pstat.setString(link.ordinal(), linkToInsert);
		    pstat.setString(referringPage.ordinal(), referringPageToInsert);
		    pstat.setString(relative.ordinal(), relativeToInsert);
		    pstat.setString(xpath.ordinal(), xpathToInsert);
		    pstat.setInt(snapshot.ordinal(), currentSnapshot);
		    pstat.setInt(LinkOccourrences.depth.ordinal(), depth);
		    pstat.setString(file.ordinal(), stored);
		    pstat.setTimestamp(date.ordinal(), sqlDate);  
		    pstat.executeUpdate();
			   
		} catch (SQLException e) {
			/* Reduce logging. */
			//log(ERROR,e.getMessage());
		}finally {
			clearResources(null, statement, pstat, null);
		}
	 
	}
	
	/**
	 * Inserts a link occurrences batch into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param linkToInsert is the link we want to add the occurrence for (absolute URL).
	 * @param referringPageToInsert is the page we found this link in.
	 * @param relativeToInsert is the relative URL of the link we are adding the occurrence for.
	 * @param xpathToInsert is the position where we found this link.
	 */
	public void insertLinkOccourrencesBatch(List<Tuple6<String, String, String, String, Integer, String>> batch) {
		PreparedStatement pstat = null;
		Connection connection = this.getConnection();
		
		try {
			/* Retrieve the current snapshot counter. */
			int currentSnapshot = this.getCurrentSequence();
		    String insertLinkOccurrenceQuery = "INSERT INTO " + LINK_OCCURRENCES_TABLE + "(link, referringPage, relativeLink, xpath, snapshot, date, depth, file) values(?,?,?,?,?,?,?,?)";	    
		    pstat = connection.prepareStatement(insertLinkOccurrenceQuery);

			for(Tuple6<String, String, String, String, Integer, String> sixth: batch) {
				String linkToInsert = sixth._1();
				String referringPageToInsert = sixth._2();
				String relativeToInsert = sixth._3();
				String xpathToInsert = sixth._4();
				int depth = sixth._5();
				String file = sixth._6();
				
				/* If xpath's null it's useless */
				if(xpathToInsert==null) 
					continue;	
				
				
			    Timestamp sqlDate = new Timestamp(new java.util.Date().getTime());
			    
			    pstat.setString(link.ordinal(), linkToInsert);
			    pstat.setString(referringPage.ordinal(), referringPageToInsert);
			    pstat.setString(relative.ordinal(), relativeToInsert);
			    pstat.setString(xpath.ordinal(), xpathToInsert);
			    pstat.setInt(snapshot.ordinal(), currentSnapshot);
			    pstat.setInt(LinkOccourrences.depth.ordinal(), depth);
			    pstat.setString(LinkOccourrences.file.ordinal(), file);
			    pstat.setTimestamp(date.ordinal(), sqlDate);  
			    pstat.addBatch();
			}
			
			pstat.executeBatch();
			
			   
		} catch (SQLException e) {
			/* Reduce logging. */
			log(ERROR,e.getMessage());
			e.printStackTrace();
		}finally {
			clearResources(connection, null, pstat, null);
		}
	 
	}
	
	/**
	 * Inserts a link collection binding into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param idLinkOccurence is the id of the bound link occurrence.
	 * @param collection is the collection we want to bind idLinkOccurrence to.
	 */
	public void updateCollections(List<Tuple2<String, Set<Long>>> bindings) {
		log(INFO,"Updating link colections...");
		Statement statement = null;
		PreparedStatement pstat = null;
		
		Connection connection = getConnection();
		
		try {
		    String insertLinkOccurrenceQuery = "INSERT INTO " + LINK_COLLECTIONS_TABLE + "(id, collection) values(?,?)";	    
		    
		    pstat = connection.prepareStatement(insertLinkOccurrenceQuery);
		    int count = 0;
		    for(Tuple2<String, Set<Long>> entry: bindings) {
		    	count++;
		    	
		    	for(Long id: entry._2) {
		    		pstat.setLong(1, id);
		    		pstat.setString(2, entry._1);
		    		pstat.addBatch();
		    	}
		    	
		    	if(count%512 == 0)
		    		pstat.executeBatch();
		    }
		    
		} catch (SQLException e) {
			/* Reduce logging. */
			e.printStackTrace();
			log(ERROR,e.getMessage());
		}finally {
			clearResources(connection, statement, pstat, null);
		}
		
		log(INFO,"Inserted Correctly.");
	 
	}
	
	public void cleanTable(String tableName) {
		Connection conn = getConnection();
		
		Statement statement = null;
		ResultSet check = null;
		try {
			statement = conn.createStatement();
			statement.executeUpdate("DELETE FROM " + tableName + " WHERE 1");
			
			check = statement.executeQuery("SELECT * FROM " + tableName);
			
			if(check.next())
				log(ERROR,"Something went wrong dropping table: " + tableName);
			else
				log(INFO,"Table " + tableName + " dropped successfully.");
		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		}finally {
			clearResources(conn, statement, null, check);
		}
	}
	
	/**
	 * s up all the database;
	 */
	public void resetData() {
		Connection conn = getConnection();
		Statement stmnt = null;
		try {
			stmnt = conn.createStatement();
			stmnt.executeUpdate("DROP DATABASE " + this.dbName);
			stmnt.executeUpdate("CREATE DATABASE " + this.dbName);
			createLinkOccourrencesTable();
			createSequence();
		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		} finally {
			clearResources(conn, stmnt, null, null);
		}
		
	}
	
	/**
	 * @return a {@link Connection} from the connection pool.
	 */
	public Connection getConnection() {
		try {	
			Connection connection = dataSource.getConnection();
			return connection;
		}catch (Exception e) {
			log(ERROR,e.getMessage());
			return null;
		}
	}
	
	/**
	 * Creates a new database for a {@link DAOPool}.
	 * @param dbName is the name of the database we want to create.
	 * @return
	 */
	protected static DAO newDAO(String dbName) {
		return new DAO(dbName);
	}
	
	public List<Tuple4<Integer, String, XPath, Integer>> getXPaths() {
		Connection conn = getConnection();
		List<Tuple4<Integer, String, XPath, Integer>> xpaths = new ArrayList<>();
		
		ResultSet result = null;
		Statement stmnt = null;
		try {
			stmnt = conn.createStatement();
			result = stmnt.executeQuery("SELECT * FROM " + LINK_OCCURRENCES_TABLE + " WHERE 1");
			
			while(result.next())
				xpaths.add(new Tuple4<>(result.getInt(1), result.getString(3), new XPath(result.getString(5)), result.getInt(6)));
		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		} finally {
			clearResources(conn, stmnt, null, result);
		}
		
		return xpaths;	
	}
	
	public List<Tuple4<Long, String, XPath, Integer>> getXPathsUntil(int snapshot) {
		Connection conn = getConnection();
		List<Tuple4<Long, String, XPath, Integer>> xpaths = new ArrayList<>();
		
		ResultSet result = null;
		Statement stmnt = null;
		try {
			stmnt = conn.createStatement();
			result = stmnt.executeQuery("SELECT * FROM " + LINK_OCCURRENCES_TABLE + " WHERE snapshot <= " + snapshot);
			
			while(result.next())
				xpaths.add(new Tuple4<>(result.getLong(1), result.getString(3), new XPath(result.getString(5)), result.getInt(6)));
		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		} finally {
			clearResources(conn, stmnt, null, result);
		}
		
		return xpaths;	
	}
	
	/**
	 * Checks whether a table exists or not.
	 * @param tableName is the table we want to check the existence.
	 * @return true if the table exists, false otherwise.
	 */
	public boolean checkTableExists(String tableName) {
		ResultSet results = null;
		Statement statement = null;
		Connection conn = getConnection();
		try {
			statement = conn.createStatement();
			results = statement.executeQuery("SHOW TABLES");

			while(results.next()) {
				String currentTable = results.getString(1);
				if(currentTable.equals(tableName))
					return true;
			}

			return false;

		} catch(Exception e) {
			log(ERROR,e.getMessage());
			return false;
		} finally {
			clearResources(conn, statement, null, results);
		}

	}
	
	public String getDatabaseName() {
		return this.dbName;
	}
	
	private void log(Level level, String message) {
		String toAppend = "[" + this.dbName + "]";
		logger.log(level, toAppend + " " + message);
	}
	
	public void eraseBenchmarkData() {	
		this.cleanTable((new Stability(this.dbName)).getCanonicalBenchmarkName());
		this.cleanTable((new PageHyperTextualReferencesDinamicity(this.dbName)).getCanonicalBenchmarkName());
		this.cleanTable((new HyperTextualContentDinamycityPlusStability(this.dbName)).getCanonicalBenchmarkName());	
	}
	
	public Long count(String tableName) {
		Statement statement = null;
		ResultSet result = null;
		Connection connection = getConnection();
		try {
			
			String  insertResultPrepared = "SELECT count(*) FROM " + tableName;
			statement = connection.createStatement();
			result = statement.executeQuery(insertResultPrepared);	
			if(result.next())
				return result.getLong(1);
			
			} catch (SQLException e1) {
				log(ERROR,e1.getMessage());
				return null;
			
			} finally {
				clearResources(connection, statement, null, result);
			}
		return null;
	}

	public void saveResults(List<BenchmarkResult> results) {
		Connection conn = this.getConnection();
		
		results.stream()
				.collect(Collectors.groupingBy(x -> x.getBenchmark()))
				.forEach((benchmark, res) -> {
					PreparedStatement stmnt = null;
					try {
						stmnt = conn.prepareStatement("INSERT INTO " + benchmark + "(snapshot, precis, recall, f1, threshold) VALUES (?,?,?,?,?)");
						
						for(BenchmarkResult r: res) {
							stmnt.setInt(1, r.getSnapshot());
							stmnt.setDouble(2, r.getPrecision());
							stmnt.setDouble(3, r.getRecall());
							stmnt.setDouble(4, r.getF1());
							stmnt.setDouble(5, r.getThreshold());
							
							stmnt.addBatch();
						}
						
						stmnt.executeBatch();
							
					} catch (SQLException e) {
						log(ERROR, e.getMessage());
					} finally {
						clearResources(null, stmnt, null, null);
					}
				});
		clearResources(conn, null, null, null);
	}
	
	public ResultSet executeQuery(String query) {
		Connection connection = this.getConnection();
		Statement stmnt = null;
		try {
			stmnt = connection.createStatement();
			return stmnt.executeQuery(query);
		}catch(SQLException e) {
			log(ERROR, e.getMessage());
			return null;
		}finally {
			clearResources(connection, stmnt, null, null);
		}
	}
	
	/*
	 * Still in testing.
	 */
	public Set<String> getURLs(int depth, int snapshot) {
		Statement statement = null;
		ResultSet result = null;
		Connection connection = getConnection();
		Set<String> urls = new HashSet<>();
		try {
			depth = depth + 1;
			System.out.println("SELECT link FROM " + LINK_OCCURRENCES_TABLE + " WHERE depth <= " + depth + " AND snapshot = " + snapshot);
			String  insertResultPrepared = "SELECT link FROM " + LINK_OCCURRENCES_TABLE + " WHERE depth <= " + depth + " AND snapshot = " + snapshot;
			statement = connection.createStatement();
			result = statement.executeQuery(insertResultPrepared);	
			
			while(result.next()) {
				urls.add(result.getString("link"));
			}
			
			} catch (SQLException e1) {
				log(ERROR,e1.getMessage());
				return urls;
			
			} finally {
				clearResources(connection, statement, null, result);
			}
		System.out.println(urls);
		return urls;
	}
}
