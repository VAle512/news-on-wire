package it.uniroma3.persistence;

import static it.uniroma3.properties.PropertiesReader.NEO4J_PASSWORD;
import static it.uniroma3.properties.PropertiesReader.NEO4J_URI;
import static it.uniroma3.properties.PropertiesReader.NEO4J_USER;

import java.util.Map;

import org.apache.log4j.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import com.google.common.collect.Maps;

import it.uniroma3.graphs.Graph;
import it.uniroma3.graphs.Node;
import it.uniroma3.properties.PropertiesReader;

/**
 * This class allows the System to communicate with a Neo4J DB.
 * @author Luigi D'Onofrio
 */
public class GraphsDAO {
	private final static Logger logger = Logger.getLogger(GraphsDAO.class);
	private final static PropertiesReader propsReader = PropertiesReader.getInstance();
	private final static String uri = propsReader.getProperty(NEO4J_URI);
	private final static String user = propsReader.getProperty(NEO4J_USER);
	private final static String password = propsReader.getProperty(NEO4J_PASSWORD);
	private static GraphsDAO instance;
	private static Driver driver;
	
	/**
	 * Constructor.
	 */
	private GraphsDAO() {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
		logger.info("Started Neo4j Driver. ");
	}
		
	/**
	 * This method writes a relationship on the DB between two {@link Node}(s).
	 * @param node1 is the left hand side {@link Node} of the relationship.
	 * @param node2is the right hand side {@link Node} of the relationship.
	 */
	public void addRelationship(Node node1, Node node2) {
		driver.session().writeTransaction(new TransactionWork<Integer>() {
		
			@Override
			public Integer execute(Transaction tx) {
				/*
				 * Let's check for node existence before adding a new node 
				 */
				Map<String, Object> map1 = findByURL(tx, node1.getName());
				Map<String, Object> map2 = findByURL(tx, node2.getName());
				
				if(map1!=null && map1.isEmpty())
					createPageNode(tx, node1.getName());
					
				if(map2!=null && map2.isEmpty())
					createPageNode(tx, node2.getName());
					
				String query = "MATCH (node1:Page {url: '"+ escape(node1.getName()) + "'}) " +
						   	   "MATCH (node2:Page {url: '"+ escape(node2.getName()) +"'}) " +
						   	   "CREATE (node1)-[:POINTS_TO]->(node2)";
				tx.run(query);
				return 1;
			}
		});
	}
	
	/**
	 * This method creates and writes on the DB a new node.
	 * @param tx is the {@link Transaction} we are including this operation in.
	 * @param url is the name of the node we want to create.
	 * @return
	 */
	private static int createPageNode(Transaction tx, String url) {	
		Map<String, Object> parameters = Maps.newHashMap();
		parameters.put("url", url);
		tx.run("CREATE (:Page {url: $url})", parameters);
	    return 1;
	}
	
	/**
	 * This method retrieves a node represented by a {@link Map}.
	 * @param tx is the {@link Transaction} we are including this operation in.
	 * @param url is the name of the node we are searching for.
	 * @return
	 */
	private static Map<String, Object> findByURL(Transaction tx, String url) {	
		Map<String, Object> parameters = Maps.newHashMap();
		parameters.put("url", url);
		StatementResult result = tx.run("MATCH (n:Page {url: $url}) return n", parameters);
		try {
			return result.single().asMap();
		}catch(Exception e) {
			return Maps.newHashMap();
		}
	}
	
	
	/**
	 * This method builds an instance of type {@link Graph} representing the graph stored in Neo4j.
	 * @return the graph represented by a {@link Graph} object.
	 */
	public static Graph getAsGraph( ) {
		Graph graph = new Graph();
		StatementResult result = driver.session().run("MATCH (n)-[r:POINTS_TO]->(b) return properties(n), properties(b)");
		result.list().stream().forEach(record ->{
			String fromNode = record.fields().get(0).value().get("name", "NO_NAME");
			String toNode = record.fields().get(1).value().get("name", "NO_NAME");
			graph.addArch(new Node(fromNode), new Node (toNode));
		});
		
		
		return graph;
	}
	
	/**
	 * This method escapes a Stirng
	 * @param command is the {@link String} to escape.
	 * @return the scaped {@link String}
	 */
	private static String escape(String command) {
		return command.replaceAll("'", "");
	}
	
	/**
	 * @return the reference to the Singleton instance of this object.
	 */
	public static GraphsDAO getInstance( ) {
		return (instance == null) ? (instance = new GraphsDAO()) : instance;
	}
	
	
	/**
	 * This method closed the Driver.
	 * This is necessary for the program to stop.
	 */
	public void close() {
		driver.close();
		logger.info("Closed Neo4j Driver");
	}

}
