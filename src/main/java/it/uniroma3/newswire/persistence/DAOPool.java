package it.uniroma3.newswire.persistence;

import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_DB_URL_PLACEHOLDER;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_JDBC_DRIVER;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_PASS;
import static it.uniroma3.newswire.properties.PropertiesReader.MYSQL_USER;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

import it.uniroma3.newswire.properties.PropertiesReader;

public class DAOPool {
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
	 * Placeholder to be replaced by the real database name.
	 */
	private static final String DB_NAME_PLACEHOLDER = propsReader.getProperty(MYSQL_DB_URL_PLACEHOLDER);
	
	private static final Logger logger = Logger.getLogger(DAOPool.class);
	public static Map<String,DAO> databaseName2DAO;
	private static DAOPool instance;

	/**
	 * Constructor.
	 */
	private DAOPool() {
		databaseName2DAO=new HashMap<>();
	}
	
	/**
	 * Retrieves a specific DAO given the name of his database. 
	 * If the database doesn't exists it will be created.
	 * 
	 * @param dbName is the name of the desired DAO.
	 * @return
	 */
	public DAO getDAO(String dbName) {
		if(databaseName2DAO.containsKey(dbName))
			return databaseName2DAO.get(dbName);
		else {
			boolean exists = checkForExistence(dbName);
			
			if(!exists) {
				logger.info("Prevented erasing.");
				//System.exit(0);
				createDB(dbName);
			}
			
			DAO dao = DAO.newDAO(dbName);
			databaseName2DAO.put(dbName, dao);
			
			if(!exists) {
				dao.createLinkOccourrencesTable();
				dao.createSequence();
			}
			return dao;
		}		
	}
	
	public int getAbsoluteMaximumSnapshot() {
		return this.getDatabasesDAOs().stream()
									  .mapToInt(dao -> dao.getCurrentSequence())
									  .max().getAsInt();
	}
	
	/**
	 * Checks for the existence of a specific database. 
	 * It returns true if the database exists, false otherwise.
	 * @param dbName
	 * @return
	 */
	private static boolean checkForExistence(String dbName) {
		boolean exists = false;
		String url = DB_URL.replace(DB_NAME_PLACEHOLDER, "");
		BasicDataSource dataSource = new BasicDataSource();
		
		dataSource.setDriverClassName(JDBC_DRIVER);
		dataSource.setUrl(url);
		dataSource.setUsername(USER);
		dataSource.setPassword(PASS);
		
		Connection conn = null;
		Statement stmnt = null;
		ResultSet dbs = null;
		try {
			
			conn = dataSource.getConnection();
			System.out.println(conn!=null);
			dbs = conn.getMetaData().getCatalogs();
			
			while(dbs.next() && !exists)
				if(dbs.getString(1).equals(dbName))
					exists = true;
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(stmnt != null)
					stmnt.close();
				if(dbs != null)
					dbs.close();
				if(dataSource != null)
					dataSource.close();
			} catch (SQLException e) {
					logger.error(e.getMessage());
					return exists;
			}
		}
		
		return exists;
	}
	
	/**
	 * Creates a database.
	 * @param dbName is the name of the database we want to create.
	 */
	private static void createDB(String dbName) {
		String url = DB_URL.replace(DB_NAME_PLACEHOLDER, "");
		
		BasicDataSource dataSource = new BasicDataSource();
		
		dataSource.setDriverClassName(JDBC_DRIVER);
		dataSource.setUrl(url);
		dataSource.setUsername(USER);
		dataSource.setPassword(PASS);
		
		Connection conn = null;
		Statement stmnt = null;
		try {
			
			conn = dataSource.getConnection();						
			stmnt = conn.createStatement();	
			stmnt.executeUpdate("CREATE DATABASE " + dbName);
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(stmnt != null)
					stmnt.close();
				if(dataSource != null)
					dataSource.close();
			} catch (SQLException e) {
					logger.error(e.getMessage());
			}
		}
	}
	
	/**
	 * Retrieves the list of all the DAOs names.
	 * @return the list of all DAOs.
	 */
	public List<String> getDatabasesDAOsByName() {
		return databaseName2DAO.keySet().stream().collect(Collectors.toList());
	}
	
	/**
	 * Retrieves the list of all the DAOs.
	 * @return the list of all DAOs.
	 */
	public List<DAO> getDatabasesDAOs() {
		return databaseName2DAO.values().stream().collect(Collectors.toList());
	}
	
	public void loadAllDAOs() {
		String[] banList = {"information_schema", "performance_schema", "sys", "mysql"};
		String url = DB_URL.replace(DB_NAME_PLACEHOLDER, "");
		BasicDataSource dataSource = new BasicDataSource();
		
		dataSource.setDriverClassName(JDBC_DRIVER);
		dataSource.setUrl(url);
		dataSource.setUsername(USER);
		dataSource.setPassword(PASS);
		
		Connection conn = null;
		Statement stmnt = null;
		ResultSet dbs = null;
		try {
			
			conn = dataSource.getConnection();
			dbs = conn.getMetaData().getCatalogs();
			
			while(dbs.next()) {
				String dbName = dbs.getString(1);
				if(!Arrays.asList(banList).contains(dbName))
					this.getDAO(dbName);
			}
				
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(stmnt != null)
					stmnt.close();
				if(dbs != null)
					dbs.close();
				if(dataSource != null)
					dataSource.close();
			} catch (SQLException e) {
					logger.error(e.getMessage());
			}
		}
		
	}
	
	/**
	 * @return a reference the the Singleton instance representing this object.
	 */
	public static DAOPool getInstance() {
		return (instance == null) ? (instance = new DAOPool()) : instance;
	}
}
