package com.castsoftware.exporter.database;

import com.castsoftware.exporter.config.getConfigValues;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;

import javax.swing.text.html.Option;
import java.util.*;
import java.util.stream.Collectors;

public class Neo4jAlUtils {


	private static final String NO_RELATIONSHIP_WEIGHT = getConfigValues.Property.NO_RELATIONSHIP_WEIGHT.toString();//NW
	private static final String NO_RELATIONSHIP = getConfigValues.Property.NO_RELATIONSHIP.toString(); //NULL
	private static final String NODE_PROP_TYPE = getConfigValues.Property.NODE_PROP_TYPE.toString();// name
	private static final String RELATIONSHIP_PROP_VALUE = getConfigValues.Property.RELATIONSHP_PROP_VALUE.toString();
	private static final String RELATIONSHIP_PROP_TYPE = getConfigValues.Property.RELATIONSHIP_PROP_TYPE.toString(); 
	private static final String NODE_LABELS = getConfigValues.Property.NODE_LABELS.toString(); 

	/**
	 * Format the string of labels
	 * 
	 * @param labels List of labels
	 * @return The String with
	 */
	private static String formatLabels(List<String> labels) {
		List<String> sb = new ArrayList<>();
		for (String l : labels) {
			sb.add(String.format("`%s`", l));
		}

		if (sb.size() == 0)
			return "";
		else
			return ":" + String.join(":", sb);
	}


	/**
	 * Format the where clause to have a string with
	 * 
	 * @param properties Properties of the object
	 * @param o          Label of the object concerned by the parameters
	 * @return
	 */
	private static String formatWhere(Map<String, Object> properties, String o) {
		List<String> sb = new ArrayList<>();
		for (Map.Entry<String, Object> l : properties.entrySet()) {
			sb.add(String.format("%s.%s=$%s", o, l.getKey(), l.getKey()));
		}
		if (sb.size() == 0)
			return " ";
		else
			return " WHERE " + String.join(" AND ", sb);
	}


	/**
	 * Find a node in the database based on its labels and properties
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param labels     Labels of the node
	 * @param properties Properties
	 * @return
	 */
	public static Optional<Node> getNode(Neo4jAl neo4jAl, List<String> labels, Map<String, Object> properties)
			throws Neo4jQueryException {

		String sLabels = formatLabels(labels);
		neo4jAl.info(sLabels); 
		String sWhereClause = formatWhere(properties, "o");
		neo4jAl.info(sWhereClause);
		String req = String.format("MATCH (o%1$s) %2$s RETURN o as node", sLabels, sWhereClause);
		neo4jAl.info(req);

		try {
			Result res = neo4jAl.executeQuery(req, properties);
			if (res.hasNext())
				return Optional.ofNullable((Node) res.next().get("node"));
			else
				return Optional.empty();
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the node. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to get the node.", e, "NEO4JUTILS");
		}
	}

	/**
	 * [modification]
	 * Find the node if exist and create a node if not exist 
	 * use MERGE to match and create node 
	 * @param neo4jAl
	 * @param headers
	 * @return
	 * @throws Neo4jQueryException
	 */
	public static Optional<Node>getNodeType(Neo4jAl neo4jAl, String headers) throws Neo4jQueryException{
		String req = String.format("MERGE (o: `%s` {%s: '%s'})  RETURN o as node", NODE_LABELS, NODE_PROP_TYPE, headers ); 

		neo4jAl.info(req);
		try{
			Result res = neo4jAl.executeQuery(req); 

			return  Optional.ofNullable((Node) res.next().get("node"));

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the node. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to get the node.", e, "NEO4JUTILS");
		}
		
	}

	/**
	 * 
	 * @param neo4jAl
	 * @param ids
	 * @return
	 * @throws Neo4jQueryException
	 */
	public static Map<String, List<Node>> sortNodesByLabel(Neo4jAl neo4jAl, List<Long> ids) throws Neo4jQueryException {
		Map<String, List<Node>> returnMap = new HashMap<>();
		String req = "MATCH (o) WHERE ID(o) IN $idList " +
				"RETURN DISTINCT o as node, LABELS(o)[0] as label";

		try {
			Map<String, Object> records;
			Result res = neo4jAl.executeQuery(req, Map.of("idList", ids));
			while (res.hasNext()) {
				records = res.next();
				returnMap.putIfAbsent((String) records.get("label"), new ArrayList<>());
				returnMap.get((String) records.get("label")).add((Node) records.get("node"));
			}

			return returnMap;
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to build the node map. Request : %s", req), e);
			throw new Neo4jQueryException("Failed to build the node map.", e, "NEO4JUTILS");
		}
	}



	/**
	 * Create a node based on a list of labels and properties
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param labels     Labels of the node
	 * @param properties Properties
	 * @return
	 * @throws Neo4jQueryException
	 */
	public static Node createNode(Neo4jAl neo4jAl, List<String> labels, Map<String, Object> properties)
			throws Neo4jQueryException {
		try {
			Node n = neo4jAl.createNode();
			labels.forEach(x -> n.addLabel(Label.label(x))); // Add the labels
			for (Map.Entry<String, Object> l : properties.entrySet()) {
				n.setProperty(l.getKey(), l.getValue());
			}

			return n;
		} catch (Exception e) {
			neo4jAl.error("Failed to create the node.", e);
			throw new Neo4jQueryException("Failed to create the node.", e, "NEO4JUTILS");
		}
	}


	/**
	 * Get a relationship
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param type    Type of the relationship
	 * @param start   Start ID
	 * @param end     End ID
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	public static Optional<Relationship> getRelationship(Neo4jAl neo4jAl, String type, Long start, Long end)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a)-[r:`%1$s`]-(b) " +
				"WHERE o.%2$s=$start AND o.%2$s=$end " +
				"RETURN r as relationship", type, Shared.TEMP_ID_VALUE);

		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req, Map.of("start", start, "end", end));
			if (res.hasNext())
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			else
				return Optional.empty();
		} catch (Exception e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}


	/**
	 * [modification]
	 * find the relationship in the neo4j dataset
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param start   Start ID
	 * @param end     End ID
	 * @param weight 
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	public static Optional<Relationship> findRelationship(Neo4jAl neo4jAl, String start, String end)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'})-[r:`%s`]->(b:`%s` {`%s` : '%s'}) "  +
				"RETURN r as relationship", NODE_LABELS, NODE_PROP_TYPE, start, RELATIONSHIP_PROP_TYPE,
				 NODE_LABELS, NODE_PROP_TYPE,end);
	
		 neo4jAl.info(req); 
 
		 try {
			 Result res = neo4jAl.executeQuery(req);
		 
			 if (res.hasNext())
				 return Optional.ofNullable((Relationship) res.next().get("relationship"));
			 else
				 return Optional.empty();
 
				 
		 } catch (Exception e) {
			 neo4jAl.error("Failed to get the relationship.", e);
			 throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		 }
	}


	
	/**
	 * [modified]
	 * if a relationship exist and weight value 
	 * exist. Update the weight value of the relationship
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param start   Start ID
	 * @param end     End ID
	 * @param weight 
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	public static Optional<Relationship> getRelationshipTypeUpdate(Neo4jAl neo4jAl, String start, String end , Double weight)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'})-[r:`%s`]->(b:`%s` {`%s` : '%s'}) SET r.`%s` = %f "  +
				"RETURN r as relationship", NODE_LABELS, NODE_PROP_TYPE, start, RELATIONSHIP_PROP_TYPE,
				 NODE_LABELS, NODE_PROP_TYPE,end,
				 RELATIONSHIP_PROP_VALUE, weight);
		
		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req);
		
			if (res.hasNext())
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			else
				return Optional.empty();

				
		} catch (Exception e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}

	/**
	 * [modified]
	 * If the a relationship exist but weight value is unknown or (NW)
	 * Update the weight value as NULL
	 * 
	 * @param neo4jAl Neo4j Access Layer
	 * @param start   Start ID
	 * @param end     End ID
	 * @param weight 
	 * @return An optional of the relationship
	 * @throws Neo4jQueryException
	 */
	public static Optional<Relationship> getRelationshipTypeUp(Neo4jAl neo4jAl, String start, String end , String weight)
			throws Neo4jQueryException {
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'})-[r:`%s`]->(b:`%s` {`%s` : '%s'}) SET r.`%s` = NULL "  +
				"RETURN r as relationship", NODE_LABELS,NODE_PROP_TYPE, start,RELATIONSHIP_PROP_TYPE,
				 NODE_LABELS, NODE_PROP_TYPE,end,
				 RELATIONSHIP_PROP_VALUE);
		
		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req);
		
			if (res.hasNext())
				return Optional.ofNullable((Relationship) res.next().get("relationship"));
			else
				return Optional.empty();

				
		} catch (Exception e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}


	/**
	 * [modified]
	 * Specific query where it 
	 * creates a new relationship, when a relationship 
	 * does not exist but have weight value 
	 * Case used when a node is renamed and want to preserve 
	 * previous relationship 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param start      Start node
	 * @param end        End node
	 * @param weight 
	 * @return
	 */
	public static Relationship createRelationshipWeight(Neo4jAl neo4jAl, String start, String end, Double weight) throws Neo4jQueryException {
		// Create the relationship
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'}), (b:`%s` {`%s` : '%s'})" +
				"MERGE (a)-[r:`%s`{`%s`: %f}]->(b) " +
				"RETURN r as relationship ", NODE_LABELS, NODE_PROP_TYPE, start, NODE_LABELS, NODE_PROP_TYPE,
				end, RELATIONSHIP_PROP_TYPE, RELATIONSHIP_PROP_VALUE, weight );

		neo4jAl.info(req); 
		neo4jAl.info(String.format("weight2: %s", weight.toString())); 

		try {

			Result res; 

			Relationship rel = null; 

			neo4jAl.info(String.format("weight: %s", weight.toString()));

	
			res = neo4jAl.executeQuery(req);

			if(!res.hasNext()){

				throw new Error("Failed to create the relationship. No return resulted"); 
			}


			else{

				rel = (Relationship) res.next().get("relationship"); 

				neo4jAl.info(rel.toString()); 

			}
			

			return rel; 

		} catch (Exception | Neo4jQueryException e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}

	/**
	 * [modified]
	 * Create relationship if weight is 'NW', 
	 * relationhip exist but no weight value
	 * query used when the nodes are renamed and want 
	 * to preserve the previous relationship 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param start      Start node
	 * @param end        End node
	 * @param weight 
	 * @return
	 */
	public static Relationship createRelationshipType(Neo4jAl neo4jAl, String start, String end) throws Neo4jQueryException {
		// Create the relationship
		String req = String.format("MATCH (a : `%s` {`%s` : '%s'}), (b:`%s` {`%s` : '%s'})" +
				"MERGE (a)-[r:`%s`]->(b) " +
				"RETURN r as relationship ", NODE_LABELS, NODE_PROP_TYPE, start, NODE_LABELS, NODE_PROP_TYPE,
				end, RELATIONSHIP_PROP_TYPE, RELATIONSHIP_PROP_VALUE);

		neo4jAl.info(req); 

		try {

			Result res; 

			Relationship rel = null; 

			res = neo4jAl.executeQuery(req);

			if(!res.hasNext()){

				throw new Error("Failed to create the relationship. No return resulted"); 
			}

			else{

				rel = (Relationship) res.next().get("relationship"); 

				neo4jAl.info(rel.toString()); 

			}
			
			return rel; 

		} catch (Exception | Neo4jQueryException e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}

	/**
	 * 
	 * Create a relationship
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param type       Type of the relationship
	 * @param start      Start ID
	 * @param end        End id
	 * @param properties List of properties
	 * @return
	 */
	public static Relationship createRelationship(Neo4jAl neo4jAl, String type, Long start, Long end,
			Map<String, Object> properties) throws Neo4jQueryException {
		// Create the relationship
		String req = String.format("MATCH (a), (b) " +
				"WHERE o.%2$s=$start AND o.%2$s=$end " +
				"MERGE (a)-[r:`%1$s`]-(b) " +
				"RETURN r as relationship ", type, Shared.TEMP_ID_VALUE);
		
		neo4jAl.info(req); 

		try {
			Result res = neo4jAl.executeQuery(req, Map.of("start", start, "end", end));
			if (!res.hasNext())
				throw new Error("Failed to create the relationship. No return resulted.");

			Relationship rel = (Relationship) res.next().get("relationship");

			// Apply properties
			for (Map.Entry<String, Object> l : properties.entrySet()) {
				rel.setProperty(l.getKey(), l.getValue());
			}

			return rel;

		} catch (Exception | Neo4jQueryException e) {
			neo4jAl.error("Failed to get the relationship.", e);
			throw new Neo4jQueryException("Failed to get the relationship", e, "NEO4JUTILS");
		}
	}



	/**
	 * [modified]
	 * Get the name of the nodes of a specific label 
	 * exist in a Neo4j datast 
	 * 
	 * @param neo4jAl    Neo4j Access Layer
	 * @param type       Type of the relationship
	 * @param start      Start ID
	 * @param end        End id
	 * @param properties List of properties
	 * @return
	 */
	public static List<String> getNodes(Neo4jAl neo4jAl) throws Neo4jQueryException {
		
		String req = String.format("MATCH (n:%s) UNWIND n.name AS name RETURN collect(DISTINCT name) as name ", NODE_LABELS);
		
		neo4jAl.info(req); 

		try{

			Result result = neo4jAl.executeQuery(req);
			neo4jAl.info(String.format("result: [%s]", result));
			if(!result.hasNext()) return new ArrayList<>();

			else return ((List<String>) result.next().get("name")).stream().sorted().collect(Collectors.toList()); 		
			
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of node"), e);
			throw new Error("Failed to get label's types");
		}


	}

	/**
	 * [modified]
	 * Delete nodes in the neo4j dataset 
	 * that does not exist in the csv file
	 * 
	 * @param neo4jAl
	 * @param name
	 * @return
	 * @throws Neo4jQueryException
	 */
	public static List<String> deleteNodes(Neo4jAl neo4jAl, String name) throws Neo4jQueryException {

		String req = String.format("MATCH (n:%s{name: '%s'}) DETACH DELETE n", NODE_LABELS, name );
		
		neo4jAl.info(req); 

		try{

			Result result = neo4jAl.executeQuery(req);
			neo4jAl.info(String.format("result: [%s]", result));
			if(!result.hasNext()) return new ArrayList<>();
	
			else return ((List<String>) result.next()); 
		
			
			
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to delete the previously existing nodes"), e);
			throw new Error("Failed to delete the nodes");
		}


	}

}
