package com.castsoftware.exporter.algorithm;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;


import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.List;

import java.util.*; 
import java.util.Optional;
import java.util.Map.Entry;

public class LabelPropagation {

    private Neo4jAl neo4jAl;

    /**
     * Get the starting node 
     * 1. Node not called
     * 2. Smallest dependencies/neighbours
     * 3. Random node 
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * 
     * 
     */
    public static String startingNode(Neo4jAl neo4jAl, String node_label, String rels_label){

        try{

           // String startNode = LabelPropagationUtils.startNode(neo4jAl, node_label, rels_label); 
           String startNodeId = LabelPropagationUtils.startNodeId(neo4jAl, node_label, rels_label); 

            //String minKey = new String(); 

            neo4jAl.info(startNodeId); 
    
            if(startNodeId.isEmpty()){ 

                startNodeId = LabelPropagation.smallestNeighbours(neo4jAl, node_label, rels_label);

                neo4jAl.info(startNodeId); 

            }

            else if (startNodeId.isEmpty()){

                startNodeId = LabelPropagation.randomNode(neo4jAl, node_label);

                neo4jAl.info(startNodeId); 
            }

            return startNodeId; 
    

        } catch (NullPointerException e){

            neo4jAl.error(String.format("Null String"), e);
			throw new Error("Failed to get the starting node");

        } 

    }

    /**
     * 3. Random Node 
     * Get a random node as  a starting node
     * @param neo4jAl
     * @param node_label
     */

    public static String randomNode(Neo4jAl neo4jAl, String node_label){
   
        List<String> nameNodes = LabelPropagationUtils.nodeId(neo4jAl , node_label);
    
        List<String> randomNode = new ArrayList<>(); 

        Random rand = new Random(); 

        //start from a random node

        int randomIndex = rand.nextInt(nameNodes.size()); 

        neo4jAl.info(String.valueOf(randomIndex));

        randomNode.add(nameNodes.get(randomIndex)); 

        String sRandomNode = String.join(",",randomNode);

        neo4jAl.info(randomNode.toString());

        return sRandomNode; 

    }

    /**
     * Get the node with smallest dependency in the graph 
     * Considering both the ingoing and outgoing 
     * neighbours of the node 
     * @param neo4jAl
     * @return 
     */
 
    public static String smallestNeighbours(Neo4jAl neo4jAl, String node_label,String rels_label){

        List<String> nameNodes = LabelPropagationUtils.nodeId(neo4jAl , node_label);

        neo4jAl.info(nameNodes.toString()); 

        Map<String, Integer> nodes = new HashMap<>(); 

        String getKey = new String(); 


        for(int i = 0; i<nameNodes.size(); i++){

            Map<String,Integer> value  = LabelPropagationUtils.smallestDependencyId(neo4jAl, node_label, nameNodes.get(i), rels_label);

            //neo4jAl.hashmap(value); 

            nodes.putAll(value);

            // neo4jAl.map(nodes); 

        }

        //  neo4jAl.map(nodes);

        if(!nodes.isEmpty()){


            Entry<String, Integer> min = null;
            for (Entry<String, Integer> entry : nodes.entrySet()) {
                if (min == null || min.getValue() > entry.getValue()) {
                    min = entry;
                }
            }

            getKey = min.getKey();
            neo4jAl.info(getKey); 

        }


        return getKey; 

    }

    /**
     * Iterate the nodes 
     * By visiting the nodes from the starting node 
     * to the next node until the graph value is 0 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     */

    public static List<String> iterateNode(Neo4jAl neo4jAl, String node_label, String rels_label){

        int configNodes= -5; 
        int configRels = -5; 
        int configGraph = 20; 

        //get the initial starting node 
        String startNode = LabelPropagation.startingNode(neo4jAl, node_label, rels_label); 
        neo4jAl.info(startNode); 

        //keep track of the visited nodes 
        List<String> visitedNodes = new ArrayList<>(); 

        //add the intital starting node in the visited node list 
        visitedNodes.add(startNode); 
        neo4jAl.info(String.format("visited nodes: %s ", visitedNodes));

        while(configGraph != 0){

            //keep track on the current node as it iterates the nodes 
            String currentNode = startNode;
            neo4jAl.info(String.format("start node : %s", currentNode)); 

            //check if an outgoing relationship is present or not 
            //Boolean checkRels = LabelPropagationUtils.getRelsExist(neo4jAl, node_label, currentNode, rels_label); 
            Boolean checkRels = LabelPropagationUtils.getRelsExistId(neo4jAl, node_label, currentNode, rels_label); 
            neo4jAl.info(checkRels.toString());

            //if outgoing relationship is present 
            if(checkRels == true){

                //decrease the graph value by 5 
                configGraph = configGraph+ configRels; 
                neo4jAl.info(String.format("configGraph[rels] : %s", configGraph)); 

                //get the neighbour of the current/starting node to be the next starting node 
                List<String>getNeighbours= LabelPropagationUtils.childNodesId(neo4jAl, node_label, currentNode, rels_label);
                neo4jAl.info(String.format("dependencies ; %s ",getNeighbours.toString()));

                
                List<String>notVisitedChildNodes = new ArrayList<>();

                neo4jAl.info(String.format("visitedNodes : %s", visitedNodes));

                //if the visited node is more than 1 check if neighbour nodes
                // are already visted nodes
                if(visitedNodes.size()>1){

                    for(String existNodes : getNeighbours){

                        if(!visitedNodes.contains(existNodes)){
    
                            notVisitedChildNodes.add(existNodes); 
    
                        }
                    }

                }

                neo4jAl.info(String.format("not Visited Child nodes : %s", notVisitedChildNodes)); 

                // if the child node have not been visited yet then it 
                // automatically be the next node 
                if(!notVisitedChildNodes.isEmpty()){
                    
                    startNode = notVisitedChildNodes.get(0); 
                    neo4jAl.info(String.format("new StartNode: %s", startNode));

                    visitedNodes.add(startNode); 
                    neo4jAl.info(String.format("visitedNodes : %s", visitedNodes.toString())); 

                    configGraph = configGraph + configNodes; 
                    neo4jAl.info(String.format("config Graph[node] : %s", String.valueOf(configGraph)));
                    

                    
                }

               //if the neighbours is more than one then choose which next node to iterate 
                else if(getNeighbours.size()>1){

                    startNode = LabelPropagation.getNeighbours(neo4jAl, node_label, currentNode, rels_label, getNeighbours); 
                    neo4jAl.info(String.format("new StartNode: %s", startNode));

                    visitedNodes.add(startNode); 
                    neo4jAl.info(String.format("visitedNodes : %s", visitedNodes.toString())); 

                    configGraph = configGraph + configNodes; 
                    neo4jAl.info(String.format("config Graph[node] : %s", String.valueOf(configGraph))); 


                }

                //if the neighbour is only one node then it automatically become the next node to iterate/starting node
                else if(getNeighbours.size() == 1){

                    startNode =  String.join(",",getNeighbours);
                    neo4jAl.info(String.format("Parent Node: %s", startNode));

                    visitedNodes.add(startNode); 
                    neo4jAl.info(String.format("visited nodes : %s", visitedNodes.toString())); 

                    neo4jAl.info("hello");
                    configGraph = configGraph + configNodes; 
                    neo4jAl.info(String.format("configGraph[node] : %s", String.valueOf(configGraph))); 

                }

            }

            else if(checkRels == false){

                neo4jAl.info("end"); 
                break;
            }

        }
        return visitedNodes;

    }

    //the child nodes of the starting/current nodes will be the parent nodes (in this case)
    //as we would iterate and decide which node we will be going next 
    //if we have two childnodes we decide which node we will
    // be going next to compare the parent nodes and its child nodes if an
    //element exist, if an element exist this proves that the parent nodes shares a same relationshop 
    //thus this parent node will be the next node it iterates 
    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param currentNode
     * @param rels_label
     * @param getNeighbours
     * @return
     */
    public static String getNeighbours(Neo4jAl neo4jAl, String node_label, String currentNode, String rels_label, List<String> getNeighbours){

        //get the neighbours/child 

        String startNode = new String(); 
    
        //iterate the child nodes of the starting/current node 
        for(String newStartNode:getNeighbours){

            neo4jAl.info(String.format("neighbors: %s", newStartNode)); 

            //get the parent node's child nodes 
            List<String> getChildNodes = LabelPropagationUtils.childNodesId(neo4jAl, node_label, newStartNode, rels_label);
            neo4jAl.info(getChildNodes.toString()); 
            List<String>notExist = new ArrayList<>();  

            //if the child nodes exist 
            if(!getChildNodes.isEmpty()){

                for(String node : getChildNodes){

                    neo4jAl.info(node); 

                    if(getNeighbours.contains(node)){

                       startNode = newStartNode; 

                    }

                    else if (!getNeighbours.contains(node)){

                        notExist.add(node); 
                        neo4jAl.info(String.format("not exist: %s", notExist.toString()));

                    }


                }

                if(!notExist.isEmpty() && startNode.isEmpty()){

                    startNode = newStartNode; 

                }
            }
            else if(getChildNodes.isEmpty()){

                break; 
            }
        }
        return startNode;

    }

    /**
     * add where it change the name all to User and relationship name to Link 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void communityGroup(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{

        //return the first community 
        List<String> firstCommunity = iterateNode(neo4jAl, node_label, rels_label); //return a list<string>
        neo4jAl.info(firstCommunity.toString());

        //get the visited Nodes 
        List<String> allVisitedNodes = new ArrayList<>(); 
        allVisitedNodes.addAll(firstCommunity);

        List<List<String>> listOfList = new ArrayList<>(); 
        listOfList.add(firstCommunity); 

        //get the name of the nodes in the graph 
        List<String> nameNodes = LabelPropagationUtils.nodeId(neo4jAl, node_label); 
        neo4jAl.info(nameNodes.toString()); 

        //loop again if all the nodes are not vistied yet 
        while(!allVisitedNodes.containsAll(nameNodes)){

            List<String>anotherNodes = new ArrayList<>(); 
            //check the other nodes that are not in the first community 
            for(String notCommunity: nameNodes){

                if(!allVisitedNodes.contains(notCommunity)){

                    anotherNodes.add(notCommunity);

                }

            }

            neo4jAl.info(anotherNodes.toString());

            String secondStartNode = getSecondStartNode(neo4jAl, node_label, rels_label, anotherNodes);

            neo4jAl.info(String.format("second start node: %s" , secondStartNode)); 

            List<String>newVisitedNode = secondIterateNode(neo4jAl, node_label, rels_label, secondStartNode);

            neo4jAl.info(String.format("first Community : %s ", firstCommunity));
            neo4jAl.info(String.format("second Community: %s", newVisitedNode)); 

            // List<String> allVisitedNodes = new ArrayList<>(); 

            // allVisitedNodes.addAll(firstCommunity); 
            allVisitedNodes.addAll(newVisitedNode);

            neo4jAl.info(String.format("all visited nodes : %s ", allVisitedNodes)); 

            listOfList.add(newVisitedNode);

            neo4jAl.info(listOfList.toString());   

        }

        //count how mant communities and create node w comID
        //match nodes belonging with the community/comID 
        int totalCommunities = listOfList.size(); 
        neo4jAl.info(String.valueOf(listOfList.size()));

        Map<String,List<String>> communities  = new HashMap<>(); 

        for(int i= 0; i<totalCommunities; i++){

            Optional<Node> nodeOptional = LabelPropagationUtils.createNode(neo4jAl, String.valueOf(i));  
            Node n = nodeOptional.get(); 

            neo4jAl.info(listOfList.get(i).toString());
            
            for(String node : listOfList.get(i)){

                Relationship r = LabelPropagationUtils.matchNodeId(neo4jAl,node_label, node , String.valueOf(i)); 

            }

            communities.put(String.valueOf(i), listOfList.get(i)); 
            neo4jAl.output(communities); 

        }

        //create a relationship between comId nodes in case if their child nodes have
        //relationship to each other 
        compareName(neo4jAl, node_label, rels_label, listOfList);

    }

    /**
     * This method is used for comparing the nodes between different communities 
     * to see if they have a relationhip to each other , if yes then the community nodes will 
     * also have a realtionship to each other. 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param listOfList
     * @throws Neo4jQueryException
     */
    private static void compareName(Neo4jAl neo4jAl, String node_label, String rels_label, List<List<String>>listOfList) throws Neo4jQueryException{

        List<String> firstComp = new ArrayList<>(); 
        List<String> secondComp = new ArrayList<>(); 

        for(int i = 0; i<listOfList.size(); i++){ 

            firstComp = listOfList.get(i); 
            neo4jAl.info(firstComp.toString()); 

            for(int j = i+1; j<listOfList.size(); j++){

            secondComp = listOfList.get(j); 
            neo4jAl.info(secondComp.toString()); 


            Relationship rels; 
                
            for (int k = 0 ; k<firstComp.size(); k++){

                for(int l = 0 ; l<secondComp.size(); l++){

                    neo4jAl.info(firstComp.get(k)); 
                    neo4jAl.info(secondComp.get(l)); 


                    Optional<Relationship>findRels = LabelPropagationUtils.findRelationshipId(neo4jAl,node_label, rels_label, firstComp.get(k), secondComp.get(l)); 
                    
                    if(findRels.isPresent()){
                       

                        rels = LabelPropagationUtils.newRelsId(neo4jAl, node_label, String.valueOf(i), String.valueOf(j));
                        
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
                    


                    Optional<Relationship>findRels2 = LabelPropagationUtils.findRelationshipId(neo4jAl,node_label, rels_label,secondComp.get(k), firstComp.get(l)); 
                    
                    if(findRels2.isPresent()){
                       

                        rels2 = LabelPropagationUtils.newRelsId(neo4jAl, node_label, String.valueOf(i), String.valueOf(j));
                        
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
     * Iterate the non-visited nodes after the first iteration
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param startNode
     * @return
     * 
     */
    public static List<String> secondIterateNode(Neo4jAl neo4jAl, String node_label, String rels_label, String startNode){

        int configNodes= -5; 
        int configRels = -5; 
        int configGraph = 20; 

        //get the initial starting node 
        neo4jAl.info(startNode); 

        //keep track of the visited nodes 
        List<String> visitedNodes = new ArrayList<>(); 

        //add the intital starting node in the visited node list 
        visitedNodes.add(startNode); 
        neo4jAl.info(String.format("visited nodes: %s ", visitedNodes));

        while(configGraph != 0){

            //keep track on the current node as it iterates the nodes 
            String currentNode = startNode;
            neo4jAl.info(String.format("start node : %s", currentNode)); 

            //check if an outgoing relationship is present or not 
            Boolean checkRels = LabelPropagationUtils.getRelsExistId(neo4jAl, node_label, currentNode, rels_label); 
            neo4jAl.info(checkRels.toString());

            //if outgoing relationship is present 
            if(checkRels == true){

                //decrease the graph value by 5 
                configGraph = configGraph+ configRels; 
                neo4jAl.info(String.format("configGraph[rels] : %s", configGraph)); 

                //get the neighbour of the current/starting node to be the next starting node 
                List<String>getNeighbours= LabelPropagationUtils.childNodesId(neo4jAl, node_label, currentNode, rels_label);
                neo4jAl.info(getNeighbours.toString());

                List<String>notVisitedChildNodes = new ArrayList<>();

                neo4jAl.info(String.format("visitedNodes : %s", visitedNodes));

                if(visitedNodes.size()>1){

                    for(String existNodes : getNeighbours){

                        if(!visitedNodes.contains(existNodes)){
    
                            notVisitedChildNodes.add(existNodes); 
    
                        }
                    }

                }
                
                neo4jAl.info(String.format("not Visited Child nodes : %s", notVisitedChildNodes)); 

                if(!notVisitedChildNodes.isEmpty()){
                    
                    startNode = notVisitedChildNodes.get(0); 
                    neo4jAl.info(String.format("new StartNode: %s", startNode));

                    visitedNodes.add(startNode); 
                    neo4jAl.info(String.format("visitedNodes : %s", visitedNodes.toString())); 

                    configGraph = configGraph + configNodes; 
                    neo4jAl.info(String.format("config Graph[node] : %s", String.valueOf(configGraph)));
                    

                    
                }
                            
                else if(getNeighbours.size()>1){

                    startNode = LabelPropagation.getNeighbours(neo4jAl, node_label, currentNode, rels_label, getNeighbours); 
                    neo4jAl.info(String.format("new StartNode: %s", startNode));

                    visitedNodes.add(startNode); 
                    neo4jAl.info(String.format("visitedNodes : %s", visitedNodes.toString())); 

                    configGraph = configGraph + configNodes; 
                    neo4jAl.info(String.format("config Graph[node] : %s", String.valueOf(configGraph))); 


                }

                else if(getNeighbours.size() == 1){

                    startNode =  String.join(",",getNeighbours);
                    neo4jAl.info(String.format("Parent Node: %s", startNode));

                    visitedNodes.add(startNode); 
                    neo4jAl.info(String.format("visited nodes : %s", visitedNodes.toString())); 

                    neo4jAl.info("hello");
                    configGraph = configGraph + configNodes; 
                    neo4jAl.info(String.format("configGraph[node] : %s", String.valueOf(configGraph))); 

                }

            }

            else if(checkRels == false){

                neo4jAl.info("end"); 
                break;
            }

        }
        return visitedNodes;

    }

    /**
     * get the second start node after the first iteration 
     * 1. get the node that is not called 
     * 2. get the node with least dependency/neighbour
     * 3. get a random node 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param nonVistedNodes
     * @return
     */
    public static String getSecondStartNode(Neo4jAl neo4jAl, String node_label, String rels_label,List<String>nonVistedNodes){
        
        try{

            //get a node that is not called 
            String startNode = LabelPropagation.secondStartNode(neo4jAl, node_label, rels_label, nonVistedNodes); 

            neo4jAl.info(startNode); 
            
            //if there are no called node, get the node with the least dependency 
            if(startNode.isEmpty()){ 

                startNode = LabelPropagation.getSecondSmallestNeighbours(neo4jAl, node_label, rels_label,nonVistedNodes);

                neo4jAl.info(startNode); 

            }

            //if there is no node with the least dependency, get a random node 
            else if (startNode.isEmpty()){

                startNode = LabelPropagation.secondRandomNode(neo4jAl, node_label, nonVistedNodes);

                neo4jAl.info(startNode); 
            }

            return startNode; 
    

        } catch (NullPointerException e){

            neo4jAl.error(String.format("Null String"), e);
			throw new Error("Failed to get the starting node");



        }

    }

    /**
     * get all the nodes that are not called in the graph 
     * and compare if the non-visited nodes exist in the list
     * if yes, the node will be the starting node 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param nonVisitedNodes
     * @return
     */
    public static String secondStartNode(Neo4jAl neo4jAl, String node_label, String rels_label, List<String>nonVisitedNodes){

        List<String> notCalledNodes = LabelPropagationUtils.secondStartNodeId(neo4jAl,node_label, rels_label);

        String startNode = new String(); 

        //compare the not called nodes with non visited nodes
        //if not called nodes exist in non-visited nodes then become 
        //the new start node 
        for(String nodes: notCalledNodes){

            if(nonVisitedNodes.contains(nodes)){

                startNode = nodes; 

            }

            else{

                startNode = new String(); 
            }

        }

        return startNode; 
    
    }

    /**
     * get the non-visited nodes with the smallest dependencies/neighbours 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @param nonVisitedNodes
     * @return
     */
    public static String getSecondSmallestNeighbours(Neo4jAl neo4jAl, String node_label,String rels_label,List<String>nonVisitedNodes){
        
        neo4jAl.info(nonVisitedNodes.toString()); 
        
        Map<String, Integer> nodes = new HashMap<>(); 


        for(int i = 0; i<nonVisitedNodes.size(); i++){

            Map<String,Integer> value  = LabelPropagationUtils.smallestDependencyId(neo4jAl, node_label, nonVisitedNodes.get(i), rels_label);

            nodes.putAll(value);

        }
        
        Entry<String, Integer> min = null;
        for (Entry<String, Integer> entry : nodes.entrySet()) {
            if (min == null || min.getValue() > entry.getValue()) {
                min = entry;
            }
        }

        String getKey = min.getKey();
        neo4jAl.info(getKey); 

        return getKey; 

    }

    /**
     * get a random non visited node
     * @param neo4jAl
     * @param node_label
     * @param nonvistedNodes
     * @return
     */
    public static String secondRandomNode(Neo4jAl neo4jAl, String node_label, List<String> nonvistedNodes ){
    
        List<String> randomNode = new ArrayList<>(); 

        Random rand = new Random(); 

        //start from a random node

        int randomIndex = rand.nextInt(nonvistedNodes.size()); 

        neo4jAl.info(String.valueOf(randomIndex));

        randomNode.add(nonvistedNodes.get(randomIndex)); 

        String sRandomNode = String.join(",",randomNode);

        neo4jAl.info(randomNode.toString());

        return sRandomNode;

    }

    /**
     * 
     * @param neo4jAl
     * @param node_label
     * @param rels_label
     * @throws Neo4jQueryException
     */
    public static void getLabelProp(Neo4jAl neo4jAl, String node_label, String rels_label) throws Neo4jQueryException{      
 
       communityGroup(neo4jAl, node_label, rels_label);
      
    }

    /**
	 * Label Propagation 
	 * @param neo4jAl
	 */
	public LabelPropagation(Neo4jAl neo4jAl) {

		this.neo4jAl = neo4jAl;
	}


    
}
