package com.castsoftware.exporter.deserialize;

import com.castsoftware.exporter.config.getConfigValues;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;

import org.apache.commons.collections.CollectionUtils;
import org.neo4j.cypher.internal.ir.CreateRelationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.swing.text.html.Option;

public class RelationshipDeserializer {

	private static final String NO_RELATIONSHIP_WEIGHT = getConfigValues.Property.NO_RELATIONSHIP_WEIGHT.toString();//NW
	private static final String NO_RELATIONSHIP = getConfigValues.Property.NO_RELATIONSHIP.toString(); //NULL
	private static final String NODE_PROP_TYPE = getConfigValues.Property.NODE_PROP_TYPE.toString();// name
	private static final String RELATIONSHIP_PROP_VALUE = getConfigValues.Property.RELATIONSHP_PROP_VALUE.toString();
	private static final String RELATIONSHIP_PROP_TYPE = getConfigValues.Property.RELATIONSHIP_PROP_TYPE.toString(); 
	private static final String NODE_LABELS = getConfigValues.Property.NODE_LABELS.toString(); 
	

	/**
	 * Create a node from a list of values
	 * @param neo4jAl Neo4j Access Layer
	 * @param headers Headers
	 * @param values Values
	 * @return
	 */
	public static Relationship mergeRelationship(Neo4jAl neo4jAl, List<String> headers, List<String> values) throws Neo4jQueryException, Exception {
		// Verify the map
		Map<String, String> zipped = Neo4jTypeMapper.zip(headers, values);

		// Get exporter parameters
		String start = Neo4jTypeMapper.verifyMap(zipped, Shared.RELATIONSHIP_START);
		String end = Neo4jTypeMapper.verifyMap(zipped, Shared.RELATIONSHIP_END);
		String sType = Neo4jTypeMapper.verifyMap(zipped, Shared.RELATIONSHIP_TYPE);

		// Transform
		Long lStart = Long.parseLong(start);
		Long lEnd = Long.parseLong(end);

		// Get the list of non null properties
		Map<String, Object> properties = new HashMap<>();
		Object neoType;
		for(String h : headers) { // Get the map of neo4j type
			if(h.equals(Shared.RELATIONSHIP_START) || h.equals(Shared.RELATIONSHIP_END) || h.equals(Shared.RELATIONSHIP_TYPE)) continue; // Skip defaults

			neoType = Neo4jTypeMapper.getNeo4jType(zipped, h);
			if( neoType != null ) properties.put(h, neoType);
		}

		// Get existing node or create
		Relationship r;
		Optional<Relationship> relationshipOptional = Neo4jAlUtils.getRelationship(neo4jAl, sType, lStart, lEnd);
		if(relationshipOptional.isEmpty()) r = Neo4jAlUtils.createRelationship(neo4jAl, sType, lStart, lEnd, properties);
		else r = relationshipOptional.get();

		return r;
	}


	/**
	 * get or create relationship based on 
	 * the csv file data 
	 * @param neo4jAl Neo4j Access Layer
	 * @param headers Headers
	 * @param values Values
	 * @return
	 */
	public static Relationship mergeRelationshipType(Neo4jAl neo4jAl, List<String> headers, List<String> values) throws Neo4jQueryException, Exception {
	
		List<String>end = new ArrayList<>(); 
		List<String>start = new ArrayList<>(); 

		List<String>lheaders= new ArrayList<>(); 
		
		String source; 
		String sEnd; 
		String weight; 
		//String weight; 
		neo4jAl.info(headers.toString()); 

		Relationship r = null; 

		try{

			for (int j = 1; j<headers.size(); j++){

				neo4jAl.info(values.toString());
				lheaders.add(headers.get(j)); 
	
				start = new ArrayList<>(); 
				start.add(values.get(0).toString());	
				source = String.join(",", start); 
	
				end = new ArrayList<>(); 
				end.add(headers.get(j).toString()); 
				sEnd = String.join(",", end); 
	
				weight =values.get(j);
	
				Optional<Relationship> relationshipPresent= Neo4jAlUtils.findRelationship(neo4jAl, source, sEnd);

				if(relationshipPresent.isPresent()){

					if(weight.equals(NO_RELATIONSHIP_WEIGHT)){

						Optional<Relationship> updateRels = Neo4jAlUtils.getRelationshipTypeUp(neo4jAl, source, sEnd, weight);
						r = updateRels.get(); 

					}

					else{

						Optional<Relationship> updateRelsWeight = Neo4jAlUtils.getRelationshipTypeUpdate(neo4jAl, source, sEnd, Double.valueOf(weight));
						r = updateRelsWeight.get(); 
					}
				}
		
				if(Optional.empty().toString().equals("Optional.empty")){
	
					if (String.valueOf(weight).equals(NO_RELATIONSHIP)) {
		
						r = null;					
					}
					else if(String.valueOf(weight).equals(NO_RELATIONSHIP_WEIGHT)) {

						r = Neo4jAlUtils.createRelationshipType(neo4jAl, source, sEnd); 
			
					}
					else{

						r = Neo4jAlUtils.createRelationshipWeight(neo4jAl, source, sEnd, Double.valueOf(weight)); 
					}
					
				}
	
			}
	
			updateNode(neo4jAl, lheaders);
			return r; 

		}catch(NumberFormatException e){
			neo4jAl.error(String.format("Wrong Cast Type. Please use Number Types"),e);
			throw new Error("Fail to get the weight of relationship");
		}catch(NullPointerException e){
			neo4jAl.error(String.format("Null Type"),e);
			throw new Error("Null Type");

		}
	}
	
	/**
	 * [modified]
	 * Deleting the nodes that are not present in the csv file 
	 * but present in the neo4j dataset 
	 * @param neo4jAl
	 * @param header
	 * @throws Neo4jQueryException
	 */

	public static void updateNode(Neo4jAl neo4jAl, List<String> header) throws Neo4jQueryException{
		
		List<String>newNode = Neo4jAlUtils.getNodes(neo4jAl); 
		List<String> elements;
		String node; 


			for(int j =0; j<newNode.size(); j++){

				if (!header.contains(newNode.get(j))) {

					elements = new ArrayList<>(); 

					elements.add(newNode.get(j));

					node = String.join(",", elements);

					Neo4jAlUtils.deleteNodes(neo4jAl, node); 


				}
			}
	}

	}






		




