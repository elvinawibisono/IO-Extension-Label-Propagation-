package com.castsoftware.exporter.utils;

import com.castsoftware.exporter.csv.Formatter;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class NodesUtils {

	/**
	 * Get the list of properties for a label
	 * @param neo4jAl Neo4j Access Layer
	 * @param label List of keys present for the label  ( sorted )
	 * @return
	 */
	public static List<String> getKeysByLabel(Neo4jAl neo4jAl, String label) {
		String request = String.format("MATCH (a:`%s`) UNWIND keys(a) AS key RETURN collect(distinct key) as keys", label);
		
		try {
			Result result = neo4jAl.executeQuery(request);
			neo4jAl.info(String.format("result : [%s] ", result));
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("keys")).stream()
					.sorted().collect(Collectors.toList());
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", label), e);
			throw new Error("Failed to get label's keys");
		}
	}

	/**
	 * Get the labels as a string list of a node
	 * @param neo4jAl Neo4j Acces Layer
	 * @param n Node to process
	 * @return
	 */
	public static List<String> getLabelsAsString(Neo4jAl neo4jAl, Node n) {
		try {
			return StreamSupport 
					.stream(n.getLabels().spliterator(), false)
					.map(Label::name)
					.collect(Collectors.toList());
		} catch (Exception e) {
			neo4jAl.error(String.format("Failed to get the list of labels for node '%d'", n.getId()), e);
			throw new Error("Failed to get node's labels");
		}
	}

	/**
	 * Get the list of values for a spefic node
	 * @param neo4jAl Neo4j Access Layer
	 * @param n Node to check
	 * @param keys Keys to validate
	 * @return
	 */
	public static List<Object> getValues(Neo4jAl neo4jAl, Node n, List<String> keys) {
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
	 * [modification]
	 * Get specific list of node's type from a specific label
	 * @param neo4jAl Neo4j Access Layer
	 * @param props Keys to validate 
	 */

	 public static List<String> getTypes(Neo4jAl neo4jAl, String label, String props){

		String request = String.format("MATCH (a:`%s`) UNWIND a.`%s` AS types RETURN collect(distinct types) as type", label,props);
		neo4jAl.info(request); 
		try{

			Result result = neo4jAl.executeQuery(request);
			neo4jAl.info(String.format("result: [%s]", result));
			if(!result.hasNext()) return new ArrayList<>();
			else return ((List<String>) result.next().get("type")).stream().sorted().collect(Collectors.toList()); 
			
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of types,'%s', for the label '%s'", props,label), e);
			throw new Error("Failed to get label's types");
		}

		}

	 }

	


