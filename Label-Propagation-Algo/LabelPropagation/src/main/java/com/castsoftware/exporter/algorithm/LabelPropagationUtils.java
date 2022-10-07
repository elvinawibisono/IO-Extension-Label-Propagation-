package com.castsoftware.exporter.algorithm;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Relationship;


import java.lang.reflect.Array;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.*;



public class LabelPropagationUtils {
    
    /**
    *  Get the list of node Ids in the graph 
    * @param neo4jAl
    * @param node_label
    * @return
    */
    public static List<String> nodeId(Neo4jAl neo4jAl, String node_label){

        List<String>nodesId = new ArrayList<>(); 

        String req =  String.format("MATCH (a:`%s`)UNWIND Id(a) AS name RETURN collect(distinct name) as name" , node_label);

        neo4jAl.info(req); 

        try {
			Result result = neo4jAl.executeQuery(req);
			neo4jAl.info(String.format("result : [%s] ", result));
			if(!result.hasNext()) {
                return new ArrayList<>();
            }

			else {
                List<Long> nodes = (List<Long>)result.next().get("name"); 

                nodesId = nodes.stream().map(s -> String.valueOf(s)).collect(Collectors.toList()); 

                neo4jAl.info(String.format("result : %s ", nodesId)); 
                
            //    return ((List<String>) result.next().get("name")).stream()
			// 		.sorted().collect(Collectors.toList());

            }

            return nodesId; 

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the nodes " ), e);
			throw new Error("Failed to get label's keys");
		}
        
    }


    /**
     * Get the starting node of the graph
     * return one node that is not called 
     * @param neo4jAl
     * @param node_label
     * @param name
     * @return
     */

    public static String startNodeId(Neo4jAl neo4jAl, String node_label, String rels_label){

        String req = String.format("MATCH(n: `%s`) WHERE NOT(n) <- [: `%s`] -() RETURN Id(n) AS name LIMIT 1", node_label, rels_label);
        
        neo4jAl.info(req);
      
        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new String();

            } 
			else {

               return (String.valueOf(result.next().get("name"))); 
               
            }

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		}


    }


    /**
     * get the total number of dependencies/neighbours of a node 
     * @param neo4jAl
     * @param node_label
     * @param name
     * @param rels_label
     * @return
     */

    public static Map<String, Integer> smallestDependencyId(Neo4jAl neo4jAl, String node_label, String Id, String rels_label){

        String req = String.format("MATCH (n:`%s`) WHERE Id(n) = %s "+
                                  "MATCH (n) -[`%s`] -(m:`%s`)" +
                                  "WITH Id(n) AS name, COUNT(DISTINCT ID(m)) AS value RETURN name, value", node_label, Id, rels_label, node_label); 

         neo4jAl.info(req);

         Map<String,Integer>output = new HashMap<>(); 

      
        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new HashMap<>();

            }

            else{

                output = new HashMap<>(); 

                while(result.hasNext()) {
                    Map<String,Object> r = result.next();
                    output.put(String.valueOf(r.get("name")), Integer.valueOf(String.valueOf(r.get("value"))));
 
                }

               // neo4jAl.output(output); 

                return output; 
            }

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'",node_label), e);
			throw new Error("Failed to get label's keys");

		}

    }

    /**
     * Check if the an outgoing relationship of a node 
     * is present or not 
     * @param neo4jAl
     * @param node_label
     * @param name
     * @param rels_label
     * @return
     */
    public static Boolean getRelsExistId(Neo4jAl neo4jAl, String node_label, String name,  String rels_label){

        String req = String.format("MATCH(n: `%s`) WHERE ID(n) = %s RETURN EXISTS ((n)-[:`%s`] ->()) AS rels", node_label, name, rels_label);
        
        neo4jAl.info(req);

      
        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return false;

            } 
			else {

               return ((Boolean)result.next().get("rels")); 
               
            }

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		}
    }

    /**
     * get a list of neighbours that have an outgoing relationship with a node 
     * @param neo4jAl
     * @param node_label
     * @param name
     * @param rels_label
     * @return
     */
    public static List<String> childNodesId(Neo4jAl neo4jAl, String node_label, String name, String rels_label){

        List<String> nodesId = new ArrayList<>(); 

        String req =  String.format("MATCH(n: `%s`)-[:`%s`] ->(m:`%s`) WHERE Id(n) = %s UNWIND Id(m) AS name RETURN COLLECT(DISTINCT name) AS name" , node_label,rels_label,node_label,name);
        neo4jAl.info(req); 
        
        try {
			Result result = neo4jAl.executeQuery(req);
			neo4jAl.info(String.format("result : [%s] ", result));
			if(!result.hasNext()){

                return new ArrayList<>();

            } 
			else{

                List<Long> nodes = (List<Long>)result.next().get("name"); 

                nodesId = nodes.stream().map(s -> String.valueOf(s)).collect(Collectors.toList()); 

            } 

            return nodesId; 

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the nodes " ), e);
			throw new Error("Failed to get label's keys");
		}
        
    }

    /**
     * create a node for the communities 
     * @param neo4jAl
     * @param name
     * @return
     * @throws Neo4jQueryException
     */
    public static Optional<Node>createNodeId(Neo4jAl neo4jAl, String name) throws Neo4jQueryException{
		String req = String.format("MERGE (o: `Community` {name: '%s'})  RETURN o as node", name ); 

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
     * Match and create a relationship between node and its community node 
     * @param neo4jAl
     * @param node_label
     * @param start
     * @param end
     * @return
     * @throws Neo4jQueryException
     */
    public static Relationship matchNodeId(Neo4jAl neo4jAl, String node_label,String start, String end) throws Neo4jQueryException{

        // Create the relationship
        String req = String.format("MATCH (a : `%s` ), (b:`Community`)" +
                            "WHERE Id(a) = %s AND b.name = '%s' " +
                            "CREATE(a)-[r:`CONNECT`]->(b) " +
                            "RETURN r as relationship ", node_label, start,end );

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
     * find if two nodes have a relationship together 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param start
     * @param end
     * @return
     * @throws Neo4jQueryException
     */
    public static Optional<Relationship> findRelationshipId(Neo4jAl neo4jAl, String node_label, String rels_label, String start, String end)
        throws Neo4jQueryException {

        String req = String.format("MATCH (a : `%s`)-[r:`%s`]->(b:`%s`) WHERE Id(a) = %s AND Id(b) = %s "  +
            "RETURN r as relationship", node_label, rels_label,node_label,start,end);

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
     * create a relationship between the community nodes 
     * @param neo4jAl
     * @param node_label
     * @param start
     * @param end
     * @return
     * @throws Neo4jQueryException
     */
    public static Relationship newRelsId(Neo4jAl neo4jAl, String node_label, String start, String end) throws Neo4jQueryException{
            // Create the relationship
        String req = String.format("MATCH (a : `Community` {name : '%s'}), (b:`Community` {name : '%s'})" +
        "MERGE (a)-[r: `CONNECT`]->(b) " +
        "RETURN r as relationship ", start,end );

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
     * Get a list of the nodes that are not called in the graph 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @return
     */
    public static List<String> secondStartNodeId(Neo4jAl neo4jAl, String node_label, String rels_label){

        List<String> nodesId = new ArrayList<>(); 

        String req = String.format("MATCH(n: `%s`) WHERE NOT(n) <- [: `%s`] -() RETURN COLLECT(DISTINCT Id(n)) AS name", node_label, rels_label);
        
        neo4jAl.info(req);

      
        try{

            Result result = neo4jAl.executeQuery(req); 

            if(!result.hasNext()){

                return new ArrayList<>();

            } 
			else {

                List<Long> nodes = (List<Long>)result.next().get("name"); 

                nodesId = nodes.stream().map(s -> String.valueOf(s)).collect(Collectors.toList()); 
               
            }

            return nodesId; 

		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of keys for the label '%s'", node_label), e);
			throw new Error("Failed to get label's keys");
		}
    }


    /**
    * Create new nodes for community nodes 
    * @param neo4jAl
    * @param name
    * @return
    * @throws Neo4jQueryException
    */
    public static Optional<Node>createNode(Neo4jAl neo4jAl, String name) throws Neo4jQueryException{
        String req = String.format("MERGE (o: `Community` {name: '%s'})  RETURN o as node", name ); 

        neo4jAl.info(req);
        try{
            Result res = neo4jAl.executeQuery(req); 

            return  Optional.ofNullable((Node) res.next().get("node"));

        } catch (Neo4jQueryException e) {
            neo4jAl.error(String.format("Failed to get the node. Request : %s", req), e);
            throw new Neo4jQueryException("Failed to get the node.", e, "NEO4JUTILS");
        }
        
    }


}
