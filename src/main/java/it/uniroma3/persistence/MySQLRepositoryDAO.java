package it.uniroma3.persistence;

import static it.uniroma3.properties.PropertiesReader.MYSQL_DB_URL;
import static it.uniroma3.properties.PropertiesReader.MYSQL_JDBC_DRIVER;
import static it.uniroma3.properties.PropertiesReader.MYSQL_LINKS_TABLE_NAME;
import static it.uniroma3.properties.PropertiesReader.MYSQL_PASS;
import static it.uniroma3.properties.PropertiesReader.MYSQL_SEQUENCE_ID;
import static it.uniroma3.properties.PropertiesReader.MYSQL_SEQUENCE_TABLE_NAME;
import static it.uniroma3.properties.PropertiesReader.MYSQL_STABILITY_TABLE_NAME;
import static it.uniroma3.properties.PropertiesReader.MYSQL_URLS_TABLE_NAME;
import static it.uniroma3.properties.PropertiesReader.MYSQL_USER;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;
import org.bson.Document;

import it.uniroma3.properties.PropertiesReader;

/**
 * This class is an interface to the MySQL relational database in which are stored all the informations
 * used in this project.
 * @author Luigi D'Onofrio
 *
 */
public class MySQLRepositoryDAO {
	private static final Logger logger = Logger.getLogger(MySQLRepositoryDAO.class);
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
	 * The name of the table used to store the URLs.
	 */
	public static final String URLS_TABLE_NAME = propsReader.getProperty(MYSQL_URLS_TABLE_NAME);
	/**
	 * The name of the table used to store the Link Occurrences.
	 */
	private static final String LINKS_TABLE_NAME = propsReader.getProperty(MYSQL_LINKS_TABLE_NAME);
	/**
	 * The name of the table used to store the results of the Stability Analysis.
	 */
	public static final String STABILITY_TABLE_NAME = propsReader.getProperty(MYSQL_STABILITY_TABLE_NAME);
	/**
	 * The name of the sequence table.
	 */
	private static final String SEQUENCE_TABLE_NAME = propsReader.getProperty(MYSQL_SEQUENCE_TABLE_NAME);
	/**
	 * The name of the sequence we want to use.
	 */
	private static final String SEQUENCE_ID = propsReader.getProperty(MYSQL_SEQUENCE_ID);
    /**
     * The Connection pool.
     */
    private BasicDataSource dataSource;
	/**
	 * The self reference to model the Singleton behavior.
	 */
	private static MySQLRepositoryDAO instance;
	
	/**
	 * Constructor.
	 */
	private MySQLRepositoryDAO() {
		try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e) {
			logger.error("Driver class not found: " + JDBC_DRIVER);
		}
		dataSource = new BasicDataSource();
		dataSource.setDriverClassName(JDBC_DRIVER);
        dataSource.setUrl(DB_URL);
        dataSource.setUsername(USER);
        dataSource.setPassword(PASS);
        dataSource.setMaxOpenPreparedStatements(16);
        dataSource.setInitialSize(24);
        dataSource.setMaxTotal(-1);
	}
	
	/**
	 * Creates (and removes, if already existing) a new table used to store URLs.
	 */
	public void createURLsTable() {
		logger.info("Creating Table [" + URLS_TABLE_NAME + "]...");

		Statement statement = null;
		Connection connection =  null;
		
		try {
			connection = getConnection();
			statement = connection.createStatement();
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE " + URLS_TABLE_NAME;
			try {
				statement.execute(dropTableQuery);
				logger.info("Old Table dropped [" + URLS_TABLE_NAME + "].");
			} catch(SQLException e) {}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + URLS_TABLE_NAME + "(id int PRIMARY KEY NOT NULL AUTO_INCREMENT, "
																		 + "url varchar(2083) NOT NULL, "
																		 + "snapshot int, "
																		 + "date Timestamp)";
			statement.execute(createTableQuery);
			logger.info("Table created [" + URLS_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement  !=  null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}

	}
	
	/**
	 * Creates (and removes, if already existing) a new table used to store link occurrences.
	 */
	public void createLinkOccourrencesTable() {
		logger.info("Creating Table [" + LINKS_TABLE_NAME + "]...");
		
		Statement statement = null;
		Connection connection = null;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE " + LINKS_TABLE_NAME;
			try {
				statement.execute(dropTableQuery);
				logger.info("Old Table dropped [" + LINKS_TABLE_NAME + "].");
			} catch(SQLException e) {
				logger.error(e.getMessage());
			}
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + LINKS_TABLE_NAME + " (id int AUTO_INCREMENT, "
																   + "link varchar(2083) NOT NULL, "
																   + "referringPage varchar(2083) NOT NULL, "
																   + "relativeLink varchar(2083), "
																   + "xpath varchar(2083), "
																   + "snapshot int NOT NULL, "
																   + "date Timestamp, "
																   + "PRIMARY KEY(id))";
			statement.execute(createTableQuery);
			logger.info("Table created [" + LINKS_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
			
		}
	}
	
	/**
	 * Creates (and removes, if already existing) a new table used to store results of the Stability Analysis.
	 */
	public void createStabilityTable() {
		logger.info("Creating Table [" + STABILITY_TABLE_NAME + "]...");

		Statement statement = null;
		Connection connection =  null;
		try {
			connection = getConnection();
			statement = connection.createStatement();
			
			/* If the table already exists simply drop it! */
			String dropTableQuery = "DROP TABLE " + STABILITY_TABLE_NAME;
			try {
				statement.execute(dropTableQuery);
				logger.info("Old Table dropped [" + STABILITY_TABLE_NAME + "].");
			} catch(SQLException e) {}
			
			/* ...and then (re)create it. */
			String createTableQuery = "CREATE TABLE " + STABILITY_TABLE_NAME + "(id int NOT NULL, "
																	  + "url varchar(2083) NOT NULL, "
																	  + "stability float NOT NULL)";
			statement.execute(createTableQuery);
			logger.info("Table created [" + STABILITY_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	/**
	 * Creates (or resets, if already existing) a new sequence used to track snapshots.
	 */
	public void createSequence() {
		logger.info("Creating Sequence [" + SEQUENCE_TABLE_NAME + "]...");

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
			logger.info("Sequence created [" + SEQUENCE_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
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
				logger.error(e.getMessage());
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
			logger.info("Sequence incremented [" + SEQUENCE_TABLE_NAME +"]");
			
		} catch (SQLException e) {
			logger.error("Sequence [" + SEQUENCE_TABLE_NAME + "] not created.");
		}finally {
			try {
				/* Releasing resources. */
				if(statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	/**
	 * Inserts an URL into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param url is the URL we want to insert.
	 */
	@SuppressWarnings("resource")
	public void insertURL(Connection connection, String url) {
		Statement statement = null;
		ResultSet result = null;
		PreparedStatement pstat = null;
		try {
			statement = connection.createStatement();
			
			/* Retrieve the current snapshot counter. */
			int snapshot = this.getCurrentSequence(); 
				
			/* If the url has already been stored for this snapshot, don't do anything. */
			String checkExistenceSameSnapshotQuery = "SELECT * FROM " +  URLS_TABLE_NAME + " WHERE url = '" + url + "' AND snapshot=" + snapshot;
			result = statement.executeQuery(checkExistenceSameSnapshotQuery);

			/* This way we can release resources. */
			if(result.next())
				throw new SQLException("URL already present");

			Timestamp currentTimestamp = new Timestamp((new java.util.Date()).getTime());
			String insertURLQuery = "INSERT INTO " + URLS_TABLE_NAME + "(url, snapshot, date) values(?,?,?)";	    
			pstat = connection.prepareStatement(insertURLQuery);
			pstat.setString(1, url);
			pstat.setInt(2, snapshot);
			pstat.setTimestamp(3, currentTimestamp);  
			pstat.executeUpdate();
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
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
	 * Inserts a result of the Stability Analysis into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param url is the result we want to insert.
	 */
	public void insertResults(Connection connection, Iterator<Document> docs) {
		PreparedStatement statement = null;
		try {
			String  insertResultPrepared = "INSERT INTO " + MySQLRepositoryDAO.STABILITY_TABLE_NAME + "(id, url, stability) VALUES (?,?,?)";
			statement = connection.prepareStatement(insertResultPrepared);
				
			final PreparedStatement finalStat = statement;
			docs.forEachRemaining(doc -> {
					try {
						finalStat.setInt(1, Integer.parseInt(doc.get("id").toString()));
						finalStat.setString(2, doc.get("url").toString());
						finalStat.setDouble(3, Double.parseDouble(doc.get("stability").toString()));
						finalStat.executeUpdate();
					} catch (SQLException e1) {
						logger.error(e1.getMessage());
						try {
							/* Releasing resources. */
							if(finalStat != null)
								finalStat.close();
							return;
						}catch(SQLException e2) {
							logger.error(e2.getMessage());
						}
					}
			});
		}catch(SQLException e3) {
			logger.error(e3.getMessage());
		
		}
	}
	
	
	/**
	 * Inserts a link occurrence into the dedicated table.
	 * @param connection is the {@link Connection} to the DB.
	 * @param link is the link we want to add the occurrence for (absolute URL).
	 * @param referringPage is the page we found this link in.
	 * @param relative is the relative URL of the link we are adding the occurrence for.
	 * @param xpath is the position where we found this link.
	 */
	public void insertLinkOccourrence(Connection connection, 
									  String link, 
									  String referringPage, 
									  String relative, 
									  String xpath) {
		Statement statement = null;
		ResultSet result = null;
		PreparedStatement pstat = null;
		try {
			/* Retrieve the current snapshot counter. */
			int snapshot = this.getCurrentSequence();
			
		    Timestamp sqlDate = new Timestamp(new java.util.Date().getTime());
		    String insertLinkOccurrenceQuery = "INSERT INTO " + LINKS_TABLE_NAME + "(link, referringPage, relativeLink, xpath, snapshot, date) values(?,?,?,?,?,?)";	    
		    pstat = connection.prepareStatement(insertLinkOccurrenceQuery);
		    pstat.setString(1, link);
		    pstat.setString(2, referringPage);
		    pstat.setString(3, relative);
		    pstat.setString(4, xpath);
		    pstat.setInt(5, snapshot);
		    pstat.setTimestamp(6, sqlDate);  
		    pstat.executeUpdate();
			   
		} catch (SQLException e) {
			logger.error(e.getMessage());
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
			logger.error(e.getMessage());
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
				logger.error(e.getMessage());
			}
		}
		return snapshot;

	}
	
	/**
	 * @return a {@link Connection} from the connection pool.
	 */
	public static Connection getConnection() {
		try {	
			Connection connection = getInstance().dataSource.getConnection();
			return connection;
		}catch (Exception e) {
			logger.error(e.getMessage());
			return null;
		}
	}
	
	/**
	 * @return a reference the the Singleton instance representing this object.
	 */
	public static MySQLRepositoryDAO getInstance() {
		return (instance == null) ? (instance = new MySQLRepositoryDAO()) : instance;
	}

}
