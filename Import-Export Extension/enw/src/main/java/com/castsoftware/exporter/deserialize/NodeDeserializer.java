package com.castsoftware.exporter.deserialize;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.Shared;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NodeDeserializer {


	/**
	 * Create a node from a list of values
	 * @param neo4jAl Neo4j Access Layer
	 * @param headers Headers
	 * @param values Values
	 * @return
	 */
	public static Node mergeNode(Neo4jAl neo4jAl, List<String> headers, List<String> values) throws Neo4jQueryException, Exception {
		// Verify the map
		Map<String, String> zipped = Neo4jTypeMapper.zip(headers, values);


		neo4jAl.info(zipped.toString()); //{name="Jameson Patterson", weight=5, Labels=["Human"], Id=8988}
		
		neo4jAl.info(headers.toString());  //[Id, Labels, name, weight]
		neo4jAl.info(values.toString());  //[8988, ["Human"], "James Patterson", 5]

		// Get exporter parameters
		Object id = Neo4jTypeMapper.verifyMap(zipped, Shared.NODE_ID);
		Object labels = Neo4jTypeMapper.verifyMap(zipped, Shared.NODE_LABELS);

		neo4jAl.info(id.toString()); //8988
		neo4jAl.info(labels.toString()); //["Human"]

		// Transform
		Long sId = Long.parseLong(id.toString());
		List<String> sLabels = Neo4jTypeMapper.getAsStringList(labels);

		neo4jAl.info(sId.toString()); //8988
		neo4jAl.info(sLabels.toString());// [Human] 

		// Get the list of non null properties
		Map<String, Object> properties = new HashMap<>();
		Object neoType;
		for(String h : headers) { // Get the map of neo4j type
			if(h.equals(Shared.NODE_ID) || h.equals(Shared.NODE_LABELS)) continue; // Skip defaults

			neo4jAl.info(h);//name //weight

			neoType = Neo4jTypeMapper.getNeo4jType(zipped, h);
			if( neoType != null ) properties.put(h, neoType);

			neo4jAl.info(properties.toString());//{name=Jameson Patterson"} //{name=Jameson Patterson", weight=5}

			neo4jAl.info(String.format("NeoType: [%s]", String.join(",", neoType.toString()))); //NeoType: [Jameson Patterson"] //NeoType: [5]
		}

		
		// Get existing node or create
		Node n;
		Optional<Node> nodeOptional = Neo4jAlUtils.getNode(neo4jAl, sLabels, properties);
		if(nodeOptional.isEmpty()) n = Neo4jAlUtils.createNode(neo4jAl, sLabels, properties);
		else n = nodeOptional.get();

		n.setProperty(Shared.TEMP_ID_VALUE, sId);
		
		return n; 
	}

	/**
	 * [modified]
	 * Create nodes based on the header of the csv file
	 * @param neo4jAl Neo4j Access Layer
	 * @param values header 
	 */
	public static Node mergeNodeType(Neo4jAl neo4jAl, List<String>values) throws Neo4jQueryException, Exception {

		List<String> header = new ArrayList<>();  
		String sHeader; 

		Node n = null;
		Optional<Node> nodeOptional; 

		for (int i = 0; i<values.size(); i++ ){

			header = new ArrayList<>(); 

			header.add(values.get(i)); 

			neo4jAl.info(header.toString()); 

			sHeader = String.join(",",header); 

			nodeOptional = Neo4jAlUtils.getNodeType(neo4jAl, sHeader);

			n = nodeOptional.get();
			
		}

		return n; 

	}



	

	

	

}
