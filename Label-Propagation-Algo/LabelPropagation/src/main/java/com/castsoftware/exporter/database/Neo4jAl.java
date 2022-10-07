package com.castsoftware.exporter.database;

import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
// import com.castsoftware.exporter.utils.ExporterUtils;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Neo4j Accces Layer
 */
public class Neo4jAl {
	private static final String EXPORTER_PREFIX = "Label Propagation : ";

	private final GraphDatabaseService db;
	private final Log log;
	private final Transaction transaction;

	/**
	 * Execute a single query
	 *
	 * @param query Cypher query to execute
	 * @return Result of the cypher query
	 * @throws Neo4jQueryException Exception during the processing of the query
	 */
	public Result executeQuery(String query) throws Neo4jQueryException {
		try {
			return this.transaction.execute(query);
		} catch (QueryExecutionException e) {
			this.error(String.format("Query produced an error. Query: '%s'", query ), e);
			throw new Neo4jQueryException(
					"Error while executing query.", query, e, "EXQS1");
		}
	}

	/**
	 * Find nodes using their Label
	 *
	 * @param label Label to search
	 * @return <code>ResourceIterator</code> Iterator on the nodes found
	 * @throws Neo4jQueryException Threw if the request produced an error
	 */
	public Iterator<Node> findNodes(Label label) throws Neo4jQueryException {
		try {
			return this.transaction.findNodes(label).stream().iterator();
		} catch (Exception e) {
			throw new Neo4jQueryException(
					String.format("Cannot find all nodes with label '%s'", label.toString()),
					e,
					"FIND1");
		}
	}

	public Iterator<Node> findNodes(List<Long> ids) throws Neo4jQueryException {
		try {
			return ids.stream().map(x -> this.transaction.getNodeById(x)).iterator();
		} catch (Exception e) {
			assert ids != null : "Cannot process null ids list";
			throw new Neo4jQueryException(
					String.format("Cannot find all nodes with ides '%s'", ids.stream().map(Object::toString).collect(Collectors.joining(", "))),
					e,
					"FIND1");
		}
	}

	/**
	 * Retrieve a node using its ID
	 *
	 * @param id ID of the node
	 * @return <code>Node</code> if the node if found, null otherwise
	 * @throws Neo4jQueryException
	 */
	public Node getNodeById(Long id) throws Neo4jQueryException {

		try {
			return this.transaction.getNodeById(id);
		} catch (NotFoundException e) {
			return null;
		} catch (Exception e) {
			throw new Neo4jQueryException("Cannot execute multiple queries", e, "GNBI2");
		}
	}

	/**
	 * Retrieve a node using its ID
	 * @throws Neo4jQueryException
	 */
	public Node createNode() throws Neo4jQueryException {

		try {
			return this.transaction.createNode();
		} catch (NotFoundException e) {
			return null;
		} catch (Exception e) {
			throw new Neo4jQueryException("Cannot create a node.", e, "GNBI2");
		}
	}

	/**
	 * Log information into the Neo4j Log File
	 * @param output Message to log
	 */
	public void info(String message) {
		log.info(EXPORTER_PREFIX + message);
	}

	/**
	 * Log info into Neo4j Map
	 * @param output
	 */
	public void output(Map<String, List<String>> output){
		log.info(EXPORTER_PREFIX + output);
	}
	/**
	 * 
	 * @param map
	 */
	public void hashmap(Map<String, Integer> map){
		log.info(EXPORTER_PREFIX + map);
	}

	/**
	 * Log info into Neo4j Map
	 * @param map
	 */
	public void print(Map<String, String> map){
		log.info(EXPORTER_PREFIX + map);
	}


	/**
	 * Log error to the Neo4j Log File
	 * @param message Message to log
	 */
	public void error(String message) {
		log.error(EXPORTER_PREFIX + message);
	}

	/**
	 * Log error to the Neo4j Log File
	 * @param message Message to log
	 * @param e Error to log
	 */
	public void error(String message, Throwable e) {
		log.error(EXPORTER_PREFIX + message, e);
	}


	public Neo4jAl(GraphDatabaseService db, Transaction transaction, Log log) {
		this.db = db;
		this.log = log;
		this.transaction = transaction;
	}


}
