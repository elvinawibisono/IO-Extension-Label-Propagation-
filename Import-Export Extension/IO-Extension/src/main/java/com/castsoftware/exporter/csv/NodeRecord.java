package com.castsoftware.exporter.csv;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.utils.NodesUtils;
import com.castsoftware.exporter.utils.Shared;

import org.neo4j.cypher.internal.expressions.functions.Labels;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Node record
 */
public class NodeRecord {

	/**
	 * Get the record for the node
	 * @param neo4jAl Neo4j Access Layer
	 * @param n Node
	 * @param keys Keys
	 * @return
	 */
	public static List<Object> getNodeRecord(Neo4jAl neo4jAl, Node n, List<String> keys) {
		List<Object> objectList = new ArrayList<>();

		// Get ID
		objectList.add(n.getId());

		// Get Label
		objectList.add(NodesUtils.getLabelsAsString(neo4jAl, n));

		// Get Values
		// Remove ID an Label from the keys passed
		List<String> subList = keys.subList(2, keys.size());
		objectList.addAll(NodesUtils.getValues(neo4jAl, n, subList));

		return objectList;
	}

	/**
	 * Get the list of header
	 * @param neo4jAl Neo4j Acces Layer
	 * @param label Label to process
	 * @return
	 */
	public static List<String> getHeaders(Neo4jAl neo4jAl, String label) {
		List<String> headers = new ArrayList<>();
		headers.add(Shared.NODE_ID);
		headers.add(Shared.NODE_LABELS);
		headers.addAll(NodesUtils.getKeysByLabel(neo4jAl, label));
		return headers;
	}

	/** 
	 * [modification]
	 * Get the list of headers by the name of nodes 
	 * @param neo4jAl Neo4j Access Layer
	 * @param label 
	 * @param prop specfic key 
	 * @return
	 */
	
	public static List<String> getTypeHeaders(Neo4jAl neo4jAl,String label, String prop){
		List<String> typeHeaders = new ArrayList<>(); 
		typeHeaders.addAll(NodesUtils.getTypes(neo4jAl,label, prop));
		return typeHeaders; 
	}

	



}


