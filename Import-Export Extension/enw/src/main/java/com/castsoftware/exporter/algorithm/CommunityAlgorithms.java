
package com.castsoftware.exporter.algorithm;
import com.castsoftware.exporter.csv.Formatter;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;


import java.lang.reflect.Array;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Relationship;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.print.attribute.standard.MediaSize.Other;

import java.util.Optional;

public class CommunityAlgorithms {

    private Neo4jAl neo4jAl;

    /**
     * Call the louvain algo 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void louvainAlgo(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

       // List<String> delete = AlgorithmsUtils.deleteGraph(neo4jAl, node_label,rels_label); 

        Map<String,String> louvain = AlgorithmsUtils.louvainAlgo(neo4jAl, node_label, rels_label);

        processOutput(neo4jAl,louvain, node_label,rels_label);

     }
    
    /**
     * Call the label propagation algo 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void labelProp(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        Map<String,String> labelProp= AlgorithmsUtils.labelPropAlgo(neo4jAl, node_label, rels_label);

        processOutput(neo4jAl,labelProp, node_label, rels_label);

    }

    /**
     * Call the WCC algo 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void weaklyAlgo(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        Map<String,String> weaklyAlgo= AlgorithmsUtils.weaklyConnected(neo4jAl, node_label, rels_label);

        processOutput(neo4jAl,weaklyAlgo, node_label,rels_label);

    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void weightLabelProp(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        Map<String, String> weightLabelProp = AlgorithmsUtils.weightLabelProp(neo4jAl, node_label, rels_label); 
        processOutput(neo4jAl, weightLabelProp, node_label,rels_label);

    }

    /**
     * 
     * @param neo4jAl
     * @param algoProcess
     * @param node_label
     * @throws Neo4jQueryException
     */
    private static void processOutput(Neo4jAl neo4jAl, Map<String,String>algoProcess, String node_label, String rels_label) throws Neo4jQueryException{

        List<String> name = new ArrayList<>(); 
        List<String> comId = new ArrayList<>(); 

        for(String key: algoProcess.keySet()){
         
           name.add(key);

           neo4jAl.info (name.toString()); 
        }

        for(String values:algoProcess.values()){
            
            comId.add(values); 

            neo4jAl.info(comId.toString());  
        }

        neo4jAl.info(comId.toString()); 

        neo4jAl.info(String.format("hello: ", comId));

        List<String>id = new ArrayList<>(); 
        List<String> getName; 
        List<String> getValues; 
        String newPropValue = new String();


        for (int i = 0; i<name.size(); i++){

            getName = new ArrayList<>(); 
            getName.add(name.get(i));
            neo4jAl.info(getName.toString()); 
            String nameOut = String.join(",",getName); 
            neo4jAl.info(nameOut);

            getValues = new ArrayList<>(); 
            getValues.add(comId.get(i)); 
            neo4jAl.info(getValues.toString());
            String idOut = String.join(",",getValues); 
            neo4jAl.info(idOut); 

            //set the properties of the nodes 
            newPropValue = AlgorithmsUtils.setProp(neo4jAl, node_label, nameOut, idOut);
            
            //returning a list of comId without repeated values 
            if(!id.contains(idOut)){
                id.add(idOut);
            }

            neo4jAl.info(id.toString()); 

        }

        List<String>node = new ArrayList<>(); 

        Map <String, List<String>> output  = new HashMap<>(); 

        for(int i = 0; i<id.size(); i++){

            List<String>nameId = AlgorithmsUtils.nodeName(neo4jAl, node_label, id.get(i));
      
            neo4jAl.info(nameId.toString()); 
            output.put(id.get(i), nameId);
            neo4jAl.info(output.get(id.get(i)).toString());
            neo4jAl.info(id.get(i));
            neo4jAl.info("line"); 
            neo4jAl.output(output);

           createNodeRels(neo4jAl, nameId, id.get(i));



           // }

        }
        neo4jAl.output(output);
        
        compareName(neo4jAl, node_label, rels_label, id, output);


    }
    
    /**
     * 
     * @param neo4jAl
     * @param nameId
     * @param idIndex
     * @throws Neo4jQueryException
     */
    private static void createNodeRels(Neo4jAl neo4jAl,List<String>nameId, String idIndex) throws Neo4jQueryException{
        
        Optional<Node> nodeOptional = AlgorithmsUtils.createNode(neo4jAl, idIndex);           
        Node n = nodeOptional.get(); 

        List<String>nameValue = new ArrayList<>(); 

        for(int j = 0; j<nameId.size(); j++){
            
            nameValue = new ArrayList<>(); 
            nameValue.add(nameId.get(j)); 
            String sName = String.join(",",nameValue); 

            Optional<Node> createNode = AlgorithmsUtils.createNode(neo4jAl,sName);           
            Node n1= createNode.get(); 

            Relationship r = AlgorithmsUtils.createRels(neo4jAl, sName , idIndex); 

        }


    }
   
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param id
     * @param output
     * @throws Neo4jQueryException
     */
    private static void compareName(Neo4jAl neo4jAl, String node_label, String rels_label, List<String> id, Map<String, List<String>> output) throws Neo4jQueryException{

    
         List<String> firstComp = new ArrayList<>(); 
         List<String> secondComp = new ArrayList<>(); 


        for(int i = 0 ; i<id.size(); i++){

            neo4jAl.info(id.get(i)); 
            firstComp = output.get(id.get(i));
       
            neo4jAl.info(firstComp.toString()); 

            for (int j =i+1; j<id.size(); j++){

                neo4jAl.info(id.get(j)); 
                secondComp = output.get(id.get(j));
                neo4jAl.info(secondComp.toString()); 

                Relationship rels; 
                
                for (int k = 0 ; k<firstComp.size(); k++){

                    for(int l = 0 ; l<secondComp.size(); l++){

                        neo4jAl.info(firstComp.get(k)); 
                        neo4jAl.info(secondComp.get(l)); 


                        Optional<Relationship>findRels = AlgorithmsUtils.findRelationship(neo4jAl,node_label, rels_label, firstComp.get(k), secondComp.get(l)); 
                        
                        if(findRels.isPresent()){
                           

                            rels = AlgorithmsUtils.newRels(neo4jAl, node_label, id.get(i), id.get(j));
                            
                        }
                            
                        else{

                            rels = null; 
                        } 

                    }

                }

                Relationship rels2; 

                for (int k = 0 ; k<secondComp.size(); k++){

                    for(int l = 0 ; l<firstComp.size(); l++){

                        neo4jAl.info(secondComp.get(k)); 
                        neo4jAl.info(firstComp.get(l)); 
                        


                        Optional<Relationship>findRels2 = AlgorithmsUtils.findRelationship(neo4jAl,node_label, rels_label,secondComp.get(k), firstComp.get(l)); 
                        
                        if(findRels2.isPresent()){
                           

                            rels2 = AlgorithmsUtils.newRels(neo4jAl, node_label, id.get(i), id.get(j));
                            
                        }
                            
                        else{

                            rels2 = null; 
                        } 

                    }

                }


            }

            



        }

        neo4jAl.info(firstComp.toString()); 
        neo4jAl.info(secondComp.toString()); 


    }
    

    /**
	 * Exporter
	 * @param neo4jAl
	 */
	public CommunityAlgorithms(Neo4jAl neo4jAl) {

		this.neo4jAl = neo4jAl;
	}





    
}


