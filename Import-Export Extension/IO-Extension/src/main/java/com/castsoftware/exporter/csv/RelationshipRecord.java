package com.castsoftware.exporter.csv;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.utils.NodesUtils;
import com.castsoftware.exporter.utils.RelationshipsUtils;
import com.castsoftware.exporter.utils.Shared;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.List;

public class RelationshipRecord {

	/**
	 * Get the record for the node
	 * @param neo4jAl Neo4j Access Layer
	 * @param n Node
	 * @param keys Keys
	 * @return
	 */
	public static List<Object> getRelationshipRecord(Neo4jAl neo4jAl, Relationship n, List<String> keys) {
		List<Object> objectList = new ArrayList<>();

		// Get start
		objectList.add(n.getType().name());
		objectList.add(n.getStartNode().getId());
		objectList.add(n.getEndNode().getId());

		// Get Values
		List<String> subList = keys.subList(3, keys.size());
		objectList.addAll(RelationshipsUtils.getValues(neo4jAl, n, subList));

		return objectList;
	}

	/**
	 * Get the record for the node
	 * @param neo4jAl Neo4j Access Layer
	 * @param n Node
	 * @param keys Keys
	 * @return
	 */
	public static List<Object> getRelationshipRecordType(Neo4jAl neo4jAl, Relationship n, List<String> keys) {
		List<Object> objectList = new ArrayList<>();

		// Get start
		objectList.add(n.getType().name());
		//objectList.add(n.getType().weight());
		
		// Get Values
		List<String> subList = keys.subList(2, keys.size());
		objectList.addAll(RelationshipsUtils.getValues(neo4jAl, n, subList));

		return objectList;
	}

	/**
	 * Get the list of header
	 * @param neo4jAl Neo4j Access Layer
	 * @param type Relationship to process
	 * @return
	 */
	public static List<String> getHeaders(Neo4jAl neo4jAl, String type) {
		List<String> headers = new ArrayList<>();
		headers.add(Shared.RELATIONSHIP_TYPE);
		headers.add(Shared.RELATIONSHIP_START);
		headers.add(Shared.RELATIONSHIP_END);
		headers.addAll(RelationshipsUtils.getKeysByType(neo4jAl, type));
		return headers;
	}

}
