package com.castsoftware.exporter.procedures;

import com.castsoftware.exporter.algorithm.LabelPropagation;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;


public class LabelPropProcedure {


    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Transaction transaction;

    /**
     * Label Propagation Algorithm Procedure 
     * @throws ProcedureException

     @Description("Label Propagation Algorithm ") +
     "Parameters : \n" +
     "               - @NodeLabel- <String> - Node labels to use. E.g. : "User" +
     "               - @RelsLabel - <String> - Relationship to Use . E.g. : "LINK" 
     "Example of use : CALL LabelPropagation("User","LINK")" +
     "")
     **/
     @Procedure(value = "LabelPropagation", mode = Mode.WRITE)
     public Stream<OutputMessage> LabelPropagation(@Name(value = "NodeLabel") String nodeLabel,
                                                @Name(value = "RelsLabel") String relsLabel
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             LabelPropagation labelProp = new LabelPropagation(neo4jAl); 
             LabelPropagation.getLabelProp(neo4jAl, nodeLabel,relsLabel);
             //return Stream.of(new algoOutput(louvain));
             return Stream.of(new OutputMessage("Create new nodes under 'Community' successfully"));
         } catch (Exception e) {
             log.error("Failed to create nodes under 'Community'.", e);
             throw new ProcedureException("Failed to create nodes. Check Neo4J logs for more details...", e);
         }

     }

}
