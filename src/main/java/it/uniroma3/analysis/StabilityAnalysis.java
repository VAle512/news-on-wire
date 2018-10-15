package it.uniroma3.analysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.bson.Document;
import org.neo4j.driver.internal.util.Iterables;

import it.uniroma3.persistence.MySQLRepositoryDAO;
import it.uniroma3.spark.SparkLoader;
import scala.Tuple3;

public class StabilityAnalysis {
	private static final Logger logger = Logger.getLogger(StabilityAnalysis.class);
	public static void analyze() {
		JavaSparkContext jsc = SparkLoader.getInstance().getContext();
		@SuppressWarnings("deprecation")
		SQLContext sqlContext = new SQLContext(jsc);
		
		JavaRDD<Document> rdd =  sqlContext.read()
									  .format("jdbc")
									  .option("url", MySQLRepositoryDAO.DB_URL)
									  .option("driver", MySQLRepositoryDAO.JDBC_DRIVER)
									  .option("dbtable", MySQLRepositoryDAO.URLS_TABLE_NAME)
									  .option("user", MySQLRepositoryDAO.USER)
									  .option("password", MySQLRepositoryDAO.PASS)
									  .load()
									  .toJavaRDD()
									  .map(row -> new Document().append("id", row.getInt(0))
											                    .append("url", row.getString(1))
											                    .append("snapshot", row.getInt(2))
											                    .append("date", row.getTimestamp(3)));
		
		int latestSnapshot = MySQLRepositoryDAO.getInstance().getCurrentSequence();
		
		JavaRDD<Document> result = rdd.map(doc -> new Tuple3<Integer, String, Integer>(Integer.parseInt(doc.get("id").toString()), 
																					   doc.get("url").toString(), 
																					   Integer.parseInt(doc.get("snapshot").toString())))
									  .groupBy(tuple -> tuple._2())
									  .map(group -> new Tuple3<>(Iterables.asList(group._2).get(0)._1().intValue(), group._1 , (double)Iterables.asList(group._2).size() / (double)latestSnapshot))
									  .map(tuple -> new Document().append("id", tuple._1()).append("url",tuple._2()).append("stability", tuple._3()));
		result.foreachPartition(partitionRdd -> {
			@SuppressWarnings("static-access")
			Connection conn = MySQLRepositoryDAO.getInstance().getConnection();
			PreparedStatement stmt = null;
			try {
				stmt = conn.prepareStatement("INSERT INTO " + MySQLRepositoryDAO.STABILITY_TABLE_NAME + "(id, url, stability) VALUES (?,?,?)");
			} catch (SQLException e1) {
				logger.error(e1.getMessage());
				return;
			}
			final PreparedStatement finalStat = stmt;
			partitionRdd.forEachRemaining(doc -> {
				try {
						finalStat.setInt(1, Integer.parseInt(doc.get("id").toString()));
						finalStat.setString(2, doc.get("url").toString());
						finalStat.setDouble(3, Double.parseDouble(doc.get("stability").toString()));
						finalStat.executeUpdate();
				
				} catch (SQLException e) {
					logger.error(e.getMessage());
					return;
				}
			});
			
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
			
		});
//		result.collect().stream().forEach(row -> {
//			try {
//				Files.write(row.toString(), new File("/home/luigi/Desktop/results"), StandardCharsets.UTF_8);
//			} catch (IOException e) {
//				System.out.println(e.getMessage());
//			}
//		});
	}
}
