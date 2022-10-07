package com.castsoftware.exporter.utils;

import com.castsoftware.exporter.config.getConfigValues;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;

import org.neo4j.cypher.internal.expressions.True;
import org.neo4j.graphdb.*;

import java.io.*; 
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RelationshipsUtils { 

	private static final String NO_RELATIONSHIP_WEIGHT = getConfigValues.Property.NO_RELATIONSHIP_WEIGHT.toString();//NW
	private static final String NO_RELATIONSHIP = getConfigValues.Property.NO_RELATIONSHIP.toString(); //NULL
	private static final String NODE_PROP_TYPE = getConfigValues.Property.NODE_PROP_TYPE.toString();// name
	private static final String RELATIONSHIP_PROP_VALUE = getConfigValues.Property.RELATIONSHP_PROP_VALUE.toString();
	private static final String RELATIONSHIP_PROP_TYPE = getConfigValues.Property.RELATIONSHIP_PROP_TYPE.toString(); 

	/**
	 * Get the list of properties for a relationship
	 * @param neo4jAl Neo4j Access Layer
	 * @param type Type of the relationship
	 * @return The list of values as object
	 */
	public static List<String> getKeysByType(Neo4jAl neo4jAl, String type) {
		String request = String.format("MATCH ()-[a:`%s`]-() UNWIND keys(a) AS key RETURN collect(distinct key) as keys", type);

		try {
			Result result = neo4jAl.executeQuery(request);
			if(!result.hasNext()) return new ArrayList<>() ;
			else return ((List<String>) result.next().get("keys")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the relationship '%s'", type), e);
			throw new Error("Failed to get relationship's keys");
		}
	}

	/**
	 * Get the list of relationship type for a label
	 * @param neo4jAl Neo4j Access Layer
	 * @param label Label to process
	 * @return The list of values as object
	 */
	public static List<String> getRelationshipTypeForLabel(Neo4jAl neo4jAl, String label) {
		String request = String.format("MATCH (o:`%s`)-[a]-() UNWIND Type(a) AS key RETURN collect(distinct key) as rels", label);

		try {
			Result result = neo4jAl.executeQuery(request);
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("rels")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list relationships for label '%s'", label), e);
			throw new Error("Failed to get the list of relationships");
		}
	}

	/**
	 * Get the list of relationship type for a label
	 * @param neo4jAl Neo4j Access Layer
	 * @param nodes List of node to process
	 * @return The list of values as object
	 */
	public static List<String> getRelationshipForNodes(Neo4jAl neo4jAl, List<Node> nodes) {
		String request = "MATCH (o)-[a]-() " +
				"WHERE ID(o) in $idList " +
				"UNWIND Type(a) AS key RETURN collect(distinct key) as rels";

		try {
			List<Long> idList = nodes.stream().map(Node::getId).collect(Collectors.toList());
			Result result = neo4jAl.executeQuery(request, Map.of("idList", idList));
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("rels")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list relationships for %d", nodes.size()), e);
			throw new Error("Failed to get the list of relationships");
		}
	}


	/**
	 * [moodified]
	 * @param neo4jAl
	 * @param label2
	 * @param type1
	 * @param label1
	 * @param type2
	 * @return
	 */
	public static List<String> getRelationship(Neo4jAl neo4jAl,String label2,String type1, String label1, String type2 ) {

		List<String>getRels = new ArrayList<>();
		
		String request = String.format("OPTIONAL MATCH (a : `%s` { %s :'%s'})-[r]->(b:`%s`{%s:'%s'})  RETURN CASE WHEN r IS NULL then 'NULL' ELSE 'EXIST' END as rels",label1,
											NODE_PROP_TYPE, type1,label2, NODE_PROP_TYPE, type2);

		neo4jAl.info(request); 

		try {
			Result result = neo4jAl.executeQuery(request);
			if(!result.hasNext()) {
				
				return new ArrayList<>();
			}
			else {

				String resultRels = String.valueOf( result.next().get("rels"));
				getRels = Arrays.asList(resultRels); 

			}

			return getRels; 
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the relationship", e));
			throw new Error("Failed to get the list of relationships");
		}
	}

	/**
	 * [modified]
	 * Get the list of relationship type for a label
	 * @param neo4jAl Neo4j Access Layer
	 * @param label1
	 * @param label2
	 * @param type1
	 * @param type2
	 * @return The list of values as object
	 */
	
	public static List<String> getRelationshipWeight(Neo4jAl neo4jAl,String label2,String type1, String label1, String type2 ) {

		List<String>relsWeight = new ArrayList<>(); 

		String request = String.format("MATCH (a : `%s` { %s :'%s'})-[r]->(b:`%s`{%s:'%s'})  RETURN CASE WHEN r.weight IS NULL then '%s' ELSE r.weight END as weight LIMIT 1",label1,
											NODE_PROP_TYPE, type1,label2, NODE_PROP_TYPE, type2, NO_RELATIONSHIP_WEIGHT);

		
		neo4jAl.info(request);
			
		try {
			Result result = neo4jAl.executeQuery(request); 

			if(!result.hasNext()){
				
				return new ArrayList<>();

			}		
			
			while(result.hasNext()){
			
				String results = String.valueOf( result.next().get("weight"));
				relsWeight = Arrays.asList(results); 

			}

			return relsWeight;

		} catch (ClassCastException e){
			neo4jAl.error(String.format("Wrong Cast Type. Please use Number Types"),e);
			throw new Error("Fail to get the weight of relationship");
		
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the weight list of relationships for label"), e);
			throw new Error("Failed to get the weight");

		}
		
	}

	/**
	 * Get the list of values for a specific node
	 * @param neo4jAl Neo4j Access Layer
	 * @param n Node to check
	 * @param keys Keys to validate
	 * @return The list of values as objects
	 */
	public static List<Object> getValues(Neo4jAl neo4jAl, Relationship n, List<String> keys) {
		try {
			List<Object> values = new ArrayList<>();
			Object it; 
			for (String k : keys) { // Parse the keys
				it = n.hasProperty(k) ? n.getProperty(k) : null;
				
				values.add(it);
			}
			return values;
		} catch (Exception e) {
			neo4jAl.error(String.format("Failed to extract values from node with id [%d] ", n.getId()), e);
			throw new Error("Failed to extract values from the node.");
		}
	}

	/**
	 * Get the list of relationship attached to the node
	 * @param n Node to process
	 * @param direction Direction of the relationship
	 * @return
	 */
	public static Set<Relationship> getRelationships(Node n, Direction direction) {
		return StreamSupport
				.stream(n.getRelationships(direction).spliterator(), false)
				.collect(Collectors.toSet());
	}

	/**
	 * Get the list of relationship attached to the node
	 * @param n Node to process
	 * @param type Type of the relation
	 * @param direction Direction
	 * @return The set of relationship
	 */
	public static Set<Relationship> getRelationships(Node n, String type, Direction direction) {
		return StreamSupport
				.stream(n.getRelationships(direction, RelationshipType.withName(type)).spliterator(), false)
				.collect(Collectors.toSet());
	}

	/**
	 * Get the list of relationship attached to the node considering the type of the other node
	 * @param n Node to process
	 * @param direction Direction of the relationship
	 * @param toConsider List of node to consider
	 * @return
	 */
	public static Set<Relationship> getRelationships(Node n,  Direction direction, List<String> toConsider) {
		return StreamSupport
				.stream(n.getRelationships(direction).spliterator(), false)
				.filter(x -> {
					// Filter nodes where at least a label is in the list
					for(Label l : x.getOtherNode(n).getLabels()) {
						if(toConsider.contains(l.name())) return true;
					}
					return false;
				})
				.collect(Collectors.toSet());
	}

	/**
	 * Get the list of relationship for a node
	 * @param n Node
	 * @param direction Direction
	 * @param toConsiderNodes List of nodes to take in account
	 * @return The list
	 */
	public static Set<Relationship> getRelationshipsFilterOnNodes(Node n,  Direction direction,
													 List<Node> toConsiderNodes) {
		return StreamSupport
				.stream(n.getRelationships(direction).spliterator(), false)
				.filter(x -> {
					// Filter nodes where at least a label is in the list
					Node oth = x.getOtherNode(n);
					return (toConsiderNodes.contains(oth));
				})
				.collect(Collectors.toSet());
	}

}
