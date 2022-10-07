package com.castsoftware.exporter.procedures;

import com.castsoftware.exporter.algorithm.CommunityAlgorithms;
import com.castsoftware.exporter.algorithm.AlgorithmsUtils;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.exceptions.file.FileIOException;
import com.castsoftware.exporter.io.NewExporter;
import com.castsoftware.exporter.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


public class AlgoProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Transaction transaction;

    /**
     * Neo4; Procedure entry point for "louvain()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save.labels(List<String>LabelsToSave, String Path, String ZipFileName, String Delimiter) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. E.g. : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.labels([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")**/
     @Procedure(value = "getLouvain", mode = Mode.WRITE)
     public Stream<OutputMessage> getLouvain(@Name(value = "NodeLabel") String nodeLabel,
                                                @Name(value = "RelsLabel") String relsLabel
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             CommunityAlgorithms louvain = new CommunityAlgorithms(neo4jAl);
             CommunityAlgorithms.louvainAlgo(neo4jAl, nodeLabel, relsLabel);
             //return Stream.of(new algoOutput(louvain));
             return Stream.of(new OutputMessage("Create new nodes under 'Community' successfully"));
         } catch (Exception e) {
             log.error("Failed to create nodes under 'Community'.", e);
             throw new ProcedureException("Failed to create nodes. Check Neo4J logs for more details...", e);
         }
     }

     /**
     * Neo4; Procedure entry point for "louvain()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save.labels(List<String>LabelsToSave, String Path, String ZipFileName, String Delimiter) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. E.g. : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.labels([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")**/
     
     @Procedure(value = "getLabelProp", mode = Mode.WRITE)
     public Stream<OutputMessage> getLabelProp(@Name(value = "NodeLabel") String nodeLabel,
                                                @Name(value = "RelsLabel") String relsLabel
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             CommunityAlgorithms labelProp = new CommunityAlgorithms(neo4jAl);
             CommunityAlgorithms.labelProp(neo4jAl, nodeLabel, relsLabel);
             //return Stream.of(new algoOutput(louvain));
             return Stream.of(new OutputMessage("Create new nodes under 'Community' successfully"));
         } catch (Exception e) {
             log.error("Failed to create nodes under 'Community'.", e);
             throw new ProcedureException("Failed to create nodes. Check Neo4J logs for more details...", e);
         }
     }

     /**
     * Neo4; Procedure entry point for "louvain()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save.labels(List<String>LabelsToSave, String Path, String ZipFileName, String Delimiter) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. E.g. : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.labels([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")

     */
     @Procedure(value = "getWCC", mode = Mode.WRITE)
     public Stream<OutputMessage> getWCC(@Name(value = "NodeLabel") String nodeLabel,
                                                @Name(value = "RelsLabel") String relsLabel
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             CommunityAlgorithms weaklyAlgo= new CommunityAlgorithms(neo4jAl);
             CommunityAlgorithms.weaklyAlgo(neo4jAl, nodeLabel, relsLabel);
             return Stream.of(new OutputMessage("Create new nodes under 'Community' successfully"));
         } catch (Exception e) {
             log.error("Failed to create nodes under 'Community'.", e);
             throw new ProcedureException("Failed to create nodes. Check Neo4J logs for more details...", e);
         }
     }

    /**
     * Neo4; Procedure entry point for "louvain()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save.labels(List<String>LabelsToSave, String Path, String ZipFileName, String Delimiter) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. E.g. : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.labels([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")

     */
    @Procedure(value = "weight.getLabelProp", mode = Mode.WRITE)
    public Stream<OutputMessage> getWeightLabelProp(@Name(value = "NodeLabel") String nodeLabel,
                                               @Name(value = "RelsLabel") String relsLabel
                                               ) throws ProcedureException{
        try {
            Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
            CommunityAlgorithms weightLabelProp= new CommunityAlgorithms(neo4jAl);
            CommunityAlgorithms. weightLabelProp(neo4jAl, nodeLabel, relsLabel);
            return Stream.of(new OutputMessage("Create new nodes under 'Community' successfully"));
        } catch (Exception e) {
            log.error("Failed to create nodes under 'Community'.", e);
            throw new ProcedureException("Failed to create nodes. Check Neo4J logs for more details...", e);
        }
    }




       /* 
     * Neo4; Procedure entry point for "louvain()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save.labels(List<String>LabelsToSave, String Path, String ZipFileName, String Delimiter) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. E.g. : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.labels([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")
     @Procedure(value = "weight.getLabelProp", mode = Mode.WRITE)
     public Stream<algoOutput> LabelProp(@Name(value = "NodeLabel") String nodeLabel,
                                                @Name(value = "RelsLabel") String relsLabel
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             Map<String,String> louvain = algorithmsUtils.louvainAlgo(neo4jAl, nodeLabel, relsLabel);
             return Stream.of(new algoOutput(louvain));
         } catch (Exception e) {
             log.error("Failed to export the list of label.", e);
             throw new ProcedureException("Failed to export the list of node. Check Neo4J logs for more details...", e);
         }
     }

     */



       //output for getLouvain procedure 

       public static class algoOutput{

        public List<String> graph; 

        public algoOutput(List<String> graph){

            this.graph = graph; 
        }
    }
    
     /**
     * Neo4J Pojo
     */
    public AlgoProcedure() { }




    
}
