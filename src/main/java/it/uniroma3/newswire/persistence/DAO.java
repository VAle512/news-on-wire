package it.uniroma3.newswire.persistence;

import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.date;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.link;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.referringPage;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.relative;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.snapshot;
import static it.uniroma3.newswire.persistence.schemas.LinkOccourrences.xpath;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL_PLACEHOLDER;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_JDBC_DRIVER;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_LINK_OCCURRENCES_TABLE_NAME;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_PASS;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_SEQUENCE_ID;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_SEQUENCE_TABLE_NAME;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_USER;
import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Level.INFO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import it.uniroma3.newswire.benchmark.benchmarks.HyperTextualContentDinamicity;
import it.uniroma3.newswire.benchmark.benchmarks.HyperTextualContentDinamycityPlusStability;
import it.uniroma3.newswire.benchmark.benchmarks.Stability;
import it.uniroma3.newswire.properties.PropertiesReader;
import it.uniroma3.newswire.utils.xpath.XPath;
import scala.Tuple2;
import scala.Tuple4;

/**
 * This class is an interface to the MySQL relational database in which are stored all the informations
 * used in this project.
 * @author Luigi D'Onofrio
 *
 */
public class DAO {	
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
	public static final String LINK_COLLECTIONS_TABLE = "LinkCollections";
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
        this.dataSource.setUrl(DB_URL.replace(DB_NAME_PLACEHOLDER, dbName));
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
			String dropTableQuery = "DROP TABLE " + LINK_OCCURRENCES_TABLE;
			try {
				statement.execute(dropTableQuery);
				log(INFO, "Old Table dropped [" + LINK_OCCURRENCES_TABLE + "].");
			} catch(SQLException e) {
				log(ERROR, e.getMessage());
			}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + LINK_OCCURRENCES_TABLE + " (id int AUTO_INCREMENT, "
																			   +   "link varchar(2083) NOT NULL, "
																			   +   "referringPage varchar(2083) NOT NULL, "
																			   +   "relativeLink varchar(2083), "
																			   +   "xpath varchar(2083), "
																			   +   "snapshot int NOT NULL, "
																			   +   "date Timestamp, "
																			   +   "PRIMARY KEY(id))";
			statement.execute(createTableQuery);
			
			log(INFO,"Table created [" + LINK_OCCURRENCES_TABLE + "].");

		} catch (SQLException e) {
			log(ERROR, e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				log(ERROR, e.getMessage());
			}
			
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
			String dropTableQuery = "DROP TABLE " + LINK_COLLECTIONS_TABLE;
			try {
				statement.execute(dropTableQuery);
				log(INFO,"Old Table dropped [" + LINK_COLLECTIONS_TABLE + "].");
			} catch(SQLException e) {}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + LINK_COLLECTIONS_TABLE + "(id int(11) NOT NULL, "
																	  			+ "collection varchar(255) NOT NULL,"
																	  			+ "FOREIGN KEY (id) REFERENCES LinkOccourrences(id))";
			statement.execute(createTableQuery);
			log(INFO,"Table created [" + LINK_COLLECTIONS_TABLE + "].");

		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
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
			String dropTableQuery = "DROP TABLE " + tableName;
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
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
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
			String dropTableQuery = "DROP TABLE " + benchmarkName;
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
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
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
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
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
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
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
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(result != null)
					result.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
		}
		return snapshot;

	}
	
	public void insertAnalysisResult(Connection connection, String tableName, Iterator<Tuple2<String, Double>> docs) {
		PreparedStatement statement = null;
		try {
			String  insertResultPrepared = "INSERT INTO " + tableName + "(url, score) VALUES (?,?)";
			statement = connection.prepareStatement(insertResultPrepared);
				
			final PreparedStatement finalStat = statement;
			docs.forEachRemaining(doc -> {
					try {
						finalStat.setString(1, doc._1);
						finalStat.setDouble(2, doc._2);
						finalStat.executeUpdate();
					} catch (SQLException e1) {
						log(ERROR,e1.getMessage());
						try {
							/* Releasing resources. */
							if(finalStat != null)
								finalStat.close();
							return;
						}catch(SQLException e2) {
							log(ERROR,e2.getMessage());
						}
					}
			});
		}catch(SQLException e3) {
			log(ERROR,e3.getMessage());
		
		}
	}
	
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
		
		PreparedStatement statement = null;
		Connection connection = getConnection();
		try {
			
			String  insertResultPrepared = "INSERT INTO " + benchmarkName + "(snapshot, precis, recall, f1, threshold) VALUES (?,?,?,?,?)";
			statement = connection.prepareStatement(insertResultPrepared);
				
			final PreparedStatement finalStat = statement;	
			try {
				finalStat.setInt(1, snapshot);
				finalStat.setDouble(2, precision);
				finalStat.setDouble(3, recall);
				finalStat.setDouble(4, f1);
				finalStat.setDouble(5, threshold);
				finalStat.executeUpdate();
			} catch (SQLException e1) {
				log(ERROR,e1.getMessage());
				try {
					/* Releasing resources. */
					if(finalStat != null)
						finalStat.close();
					if(statement != null)
						statement.close();
					if(connection != null);
						connection.close();
					return;
				}catch(SQLException e2) {
					log(ERROR,e2.getMessage());
				}
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
				try {
					/* Releasing resources. */
					if(statement != null)
						statement.close();
					if(connection != null);
						connection.close();
					if(result != null);
						result.close();
					return null;
				}catch(SQLException e2) {
					log(ERROR,e2.getMessage());
					return null;
				}
			}
		return null;
	}
	
	/**
	 * Inserts a link occurrence into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param linkToInsert is the link we want to add the occurrence for (absolute URL).
	 * @param referringPageToInsert is the page we found this link in.
	 * @param relativeToInsert is the relative URL of the link we are adding the occurrence for.
	 * @param xpathToInsert is the position where we found this link.
	 */
	public void insertLinkOccourrence(Connection connection, String linkToInsert, String referringPageToInsert, String relativeToInsert, String xpathToInsert) {
		Statement statement = null;
		ResultSet result = null;
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
		    
		    String insertLinkOccurrenceQuery = "INSERT INTO " + LINK_OCCURRENCES_TABLE + "(link, referringPage, relativeLink, xpath, snapshot, date) values(?,?,?,?,?,?)";	    
		    
		    pstat = connection.prepareStatement(insertLinkOccurrenceQuery);
		    
		    pstat.setString(link.ordinal(), linkToInsert);
		    pstat.setString(referringPage.ordinal(), referringPageToInsert);
		    pstat.setString(relative.ordinal(), relativeToInsert);
		    pstat.setString(xpath.ordinal(), xpathToInsert);
		    pstat.setInt(snapshot.ordinal(), currentSnapshot);
		    pstat.setTimestamp(date.ordinal(), sqlDate);  
		    pstat.executeUpdate();
			   
		} catch (SQLException e) {
			/* Reduce logging. */
			//log(ERROR,e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(result != null)
					result.close();
				if(pstat != null)
					pstat.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	 
	}
	
	/**
	 * Inserts a link collection binding into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param idLinkOccurence is the id of the bound link occurrence.
	 * @param collection is the collection we want to bind idLinkOccurrence to.
	 */
	public void updateCollections(List<Tuple2<String, Set<Integer>>> bindings) {
		log(INFO,"Updating link colections...");
		Statement statement = null;
		ResultSet result = null;
		PreparedStatement pstat = null;
		
		Connection connection = getConnection();
		
		try {
		    String insertLinkOccurrenceQuery = "INSERT INTO " + LINK_COLLECTIONS_TABLE + "(id, collection) values(?,?)";	    
		    
		    pstat = connection.prepareStatement(insertLinkOccurrenceQuery);
		    int count = 0;
		    for(Tuple2<String, Set<Integer>> entry: bindings) {
		    	count++;
		    	
		    	for(Integer id: entry._2) {
		    		pstat.setInt(1, id);
		    		pstat.setString(2, entry._1);
		    		pstat.addBatch();
		    	}
		    	
		    	if(count%512 == 0)
		    		pstat.executeBatch();
		    }
		    
		} catch (SQLException e) {
			/* Reduce logging. */
			log(ERROR,e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(result != null)
					result.close();
				if(pstat != null)
					pstat.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
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
			try {
				/* Releasing resources. */
				if(conn != null)
					conn.close();
				if(check != null)
					check.close();
				if(statement != null)
					statement.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Cleans up all the database;
	 */
	public void resetData() {
		Connection conn = getConnection();
		Statement stmnt = null;
		try {
			stmnt = conn.createStatement();
			stmnt.executeQuery("SET FOREIGN_KEY_CHECKS=0");
			createLinkOccourrencesTable();
			createSequence();
		} catch (SQLException e) {
			log(ERROR,e.getMessage());
		} finally {
			try {
				stmnt.executeQuery("SET FOREIGN_KEY_CHECKS=1");
				if(conn != null)
					conn.close();
				if(stmnt != null)
					stmnt.close();
			} catch (SQLException e) {
				log(ERROR,e.getMessage());
			}
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
			try {
				if(results != null)
					results.close();
				if(statement != null)
					statement.close();
				if (conn != null)
					conn.close();
			} catch(SQLException e) {
				log(ERROR,e.getMessage());
			}
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
		this.cleanTable((new HyperTextualContentDinamicity(this.dbName)).getCanonicalBenchmarkName());
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
				try {
					/* Releasing resources. */
					if(statement != null)
						statement.close();
					if(connection != null);
						connection.close();
					if(result != null);
						result.close();
					return null;
				}catch(SQLException e2) {
					log(ERROR,e2.getMessage());
					return null;
				}
			}
		return null;
	}
}
