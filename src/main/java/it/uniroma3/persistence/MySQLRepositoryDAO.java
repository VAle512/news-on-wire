package it.uniroma3.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

public class MySQLRepositoryDAO {
	private static final Logger logger = Logger.getLogger(MySQLRepositoryDAO.class);
	public static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
	public static final String DB_URL = "jdbc:mysql://localhost:3306/urls?verifyServerCertificate=false&useSSL=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	public static final String USER = "luigi";
	public static final String PASS = "0lolo0";
	public static final String URLS_TABLE_NAME = "URLs";
	public static final String STABILITY_TABLE_NAME = "Stability";
	private static final String LINKS_TABLE_NAME = "LinkOccourrences";
	private static final String SEQUENCE_NAME = "sequence";
	private static final String SEQUENCE_ID = "snapshot";
    private static BasicDataSource dataSource;
	private static MySQLRepositoryDAO instance;
	
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
        dataSource.setInitialSize(100);
	}

	public void createURLsTable() {
		logger.info("Creating Table [" + URLS_TABLE_NAME + "]...");

		Statement stmt = null;
		Connection conn =  null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			String dropTable = "DROP TABLE " + URLS_TABLE_NAME;

			try {
				stmt.execute(dropTable);
				logger.info("Old Table dropped [" + URLS_TABLE_NAME + "].");
			} catch(SQLException e) {
				logger.error(e.getMessage());
			}

			String sqlQuery = "CREATE TABLE " + URLS_TABLE_NAME +"(id int PRIMARY KEY NOT NULL AUTO_INCREMENT, url varchar(2083) NOT NULL, snapshot int, date Timestamp)";
			stmt.execute(sqlQuery);

			logger.info("Table created [" + URLS_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}

	}
	
	public void createStabilityTable() {
		logger.info("Creating Table [" + STABILITY_TABLE_NAME + "]...");

		Statement stmt = null;
		Connection conn =  null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();

			String dropTable = "DROP TABLE " + STABILITY_TABLE_NAME;

			try {
				stmt.execute(dropTable);
				logger.info("Old Table dropped [" + STABILITY_TABLE_NAME + "].");
			} catch(SQLException e) {
				logger.error(e.getMessage());
			}

			String sqlQuery = "CREATE TABLE " + STABILITY_TABLE_NAME + "(id int NOT NULL, url varchar(2083) NOT NULL, stability float NOT NULL)";
			stmt.execute(sqlQuery);

			logger.info("Table created [" + STABILITY_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	public void createLinkOccourrencesTable() {
		logger.info("Creating Table [" + LINKS_TABLE_NAME + "]...");
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();

			String dropTable = "DROP TABLE " + LINKS_TABLE_NAME;
			
			try {
				stmt.execute(dropTable);
				logger.info("Old Table dropped [" + LINKS_TABLE_NAME + "].");
			} catch(SQLException e) {
				logger.error(e.getMessage());
			}

			String sqlQuery = "CREATE TABLE " + LINKS_TABLE_NAME + " (id int AUTO_INCREMENT, link varchar(2083) NOT NULL, referringPage varchar(2083) NOT NULL, relativeLink varchar(2083), xpath varchar(2083), snapshot int NOT NULL, date Timestamp, PRIMARY KEY(id))";

			stmt.execute(sqlQuery);

			logger.info("Table created [" + LINKS_TABLE_NAME + "].");

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
			
		}
	}
	
	public void createSequence() {
		logger.info("Creating Sequence [" + SEQUENCE_NAME + "]...");

		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();


			String sqlQuery = " CREATE TABLE " + SEQUENCE_NAME + " (id varchar(255) PRIMARY KEY NOT NULL, sequence_value int)";
			stmt.execute(sqlQuery);

			sqlQuery = "INSERT INTO " + SEQUENCE_NAME + " VALUES ('" + SEQUENCE_ID + "', 0)";
			stmt.execute(sqlQuery);

			stmt.execute("UPDATE " + SEQUENCE_NAME + " SET sequence_value = 0 WHERE id = '" + SEQUENCE_ID + "'");
			logger.info("Sequence created [" + SEQUENCE_NAME + "].");

			stmt.close();
			conn.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	public void incrementSequence() {
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			
			stmt.executeUpdate("UPDATE " + SEQUENCE_NAME + " SET sequence_value = sequence_value + 1 WHERE id = '" + SEQUENCE_ID + "'");
			logger.info("Sequence incremented [" + SEQUENCE_NAME +"]");
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	@SuppressWarnings("resource")
	public void insertURL(Connection conn, String url) {
		Statement stmt = null;
		ResultSet sequenceStmt = null;
		PreparedStatement pstat = null;
		try {
			stmt = conn.createStatement();

			sequenceStmt = stmt.executeQuery("SELECT sequence_value FROM " +  SEQUENCE_NAME+ " WHERE id = '"+ SEQUENCE_ID + "'");

			if(sequenceStmt.next()) {
				int snapshot = sequenceStmt.getInt(1);  

				sequenceStmt = stmt.executeQuery("SELECT * FROM " +  URLS_TABLE_NAME + " WHERE url = '"+ url + "' AND snapshot=" + snapshot);
				if(sequenceStmt.next())
					return;

				java.util.Date myDate = new java.util.Date();
				Timestamp sqlDate = new Timestamp(myDate.getTime());
				String sqlQuery = "INSERT INTO " + URLS_TABLE_NAME + "(url, snapshot, date) values(?,?,?)";	    
				pstat = conn.prepareStatement(sqlQuery);
				pstat.setString(1, url);
				pstat.setInt(2, snapshot);
				pstat.setTimestamp(3, sqlDate);  
				pstat.executeUpdate();
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				sequenceStmt.close();
				pstat.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}

	}
	
	public void insertLinkOccourrence(Connection conn, String link, String referringPage, String relative, String xpath) {
		Statement stmt = null;
		ResultSet sequenceStmt = null;
		PreparedStatement pstat = null;
		try {
			stmt = conn.createStatement();
			sequenceStmt = stmt.executeQuery("SELECT sequence_value FROM " +  SEQUENCE_NAME+ " WHERE id = '"+ SEQUENCE_ID + "'");
		
			if(sequenceStmt.next()) {
				int snapshot = sequenceStmt.getInt(1);  
			    java.util.Date myDate = new java.util.Date();
			    Timestamp sqlDate = new Timestamp(myDate.getTime());
			    String sqlQuery = "INSERT INTO " + LINKS_TABLE_NAME + "(link, referringPage, relativeLink, xpath, snapshot, date) values(?,?,?,?,?,?)";	    
			    pstat = conn.prepareStatement(sqlQuery);
			    pstat.setString(1, link);
			    pstat.setString(2, referringPage);
			    pstat.setString(3, relative);
			    pstat.setString(4, xpath);
			    pstat.setInt(5, snapshot);
			    pstat.setTimestamp(6, sqlDate);  
			    pstat.executeUpdate();

			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}finally {
			try {
				stmt.close();
				sequenceStmt.close();
				pstat.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	 
	}
	
	public int getCurrentSequence() {
		Statement stmt = null;
		ResultSet sequenceStmt = null;
		Connection conn = null;
		int snapshot = 0;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			sequenceStmt = stmt.executeQuery("SELECT sequence_value FROM " +  SEQUENCE_NAME+ " WHERE id = '"+ SEQUENCE_ID + "'");

			if(sequenceStmt.next()) 
				snapshot = sequenceStmt.getInt(1);  

		} catch (SQLException e) {
			logger.error(e.getMessage());
			return -1;
		}finally {
			try {
				stmt.close();
				sequenceStmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		return snapshot;

	}
	
	public static MySQLRepositoryDAO getInstance() {
		return (instance==null) ? (instance = new MySQLRepositoryDAO()) : instance;
	}
	
	public static Connection getConnection() {
		try {
			return dataSource.getConnection();
		}catch (Exception e) {
			logger.error(e.getMessage());
			return null;
		}
	}
	
	
	
}
