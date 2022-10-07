package com.castsoftware.exporter.procedures;

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
import java.util.List;
import java.util.stream.Stream;

public class SaverProcedure {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Transaction transaction;

    /**
     * Neo4; Procedure entry point for "fexporter.save.labels()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save.labels(List<String>LabelsToSave, String Path, String ZipFileName, String Delimiter) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <String List> - Labels to save, as a list of string. E.g. : [\"C_relationship\", \"F_FrameworkRule\"] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.labels([\"C_relationship\", \"F_FrameworkRule\"], \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")**/
     @Procedure(value = "fexporter.save.labels", mode = Mode.WRITE)
     public Stream<OutputMessage> saveLabelProcedure(@Name(value = "LabelsToSave") List<String> labelList,
                                                @Name(value = "Path") String path,
                                                @Name(value = "ZipFileName",defaultValue="export") String zipFileName,
                                                @Name(value = "Delimiter", defaultValue=";") String delimiter
                                                ) throws ProcedureException{
         try {
             Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
             NewExporter exporter = new NewExporter(neo4jAl, delimiter);
             Path output = exporter.exportLabelList(path, zipFileName, labelList);
             return Stream.of(new OutputMessage(String.format("A new zip file has been created under '%s'.", output.toString())));
         } catch (Exception | FileIOException e) {
             log.error("Failed to export the list of label.", e);
             throw new ProcedureException("Failed to export the list of node. Check Neo4J logs for more details...", e);
         }
     }


    /**
     * Neo4j Procedure entry point for "fexporter.save()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save(NodesToSave, Path, ZipFileName, SaveRelationship, ConsiderNeighbors) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @NodesToSave- <Long String> - ID List to save, as a list of long. E.g. : [ 10002, 10004] " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.nodes([ 10002, 10004], \"C:/Neo4j_exports/\", \"MyReport\", ",")" +
     "")**/
    @Procedure(value = "fexporter.save.nodes", mode = Mode.WRITE)
    public Stream<OutputMessage> saveNodeProcedure(@Name(value = "NodesToSave") List<Long> nodeList,
                                               @Name(value = "Path") String path,
                                               @Name(value = "ZipFileName",defaultValue="export") String zipFileName,
                                               @Name(value = "Delimiter", defaultValue=";") String delimiter
    ) throws ProcedureException{
        try {
            Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
            NewExporter exporter = new NewExporter(neo4jAl, delimiter);
            Path output = exporter.exportIdListString(path, zipFileName, nodeList);
            return Stream.of(new OutputMessage(String.format("A new zip file has been created under '%s'.", output.toString())));
        } catch (Exception | FileIOException e) {
            log.error("Failed to export the list of nodes.", e);
            throw new ProcedureException("Failed to export the list of node. Check Neo4J logs for more details...", e);
        }
    }

    /**
     * [modification]
     * Neo4j Procedure entry point for "fexporter.save()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.save(NodesToSave, Path, ZipFileName, SaveRelationship, ConsiderNeighbors) - Save labels to CSV file format. \n" +
     "Parameters : \n" +
     "               - @LabelsToSave- <Long String> - Labels to save e.g ["Human"] " 
     "               - @Properties -<String> - Specific property user want to extract. E.g. : name " +
     "               - @Path - <String> - Location to save output results. E.g. : \"C:\\User\\John\"" +
     "               - @ZipFileName - <String> - Name of the final zip file (the extension .zip will be automatically added). E.g. : \"Result_05_09\" " +
     "               - @Delimiter - <String> - CSV delimiting character " +
     "Example of use : CALL fexporter.save.types(Person, name ,  \"C:/Neo4j_exports/\", \"MyReport\", "," )" +
     "")**/
    @Procedure(value = "fexporter.save.types", mode = Mode.WRITE)
    public Stream<OutputMessage> saveTypesProcedure(@Name(value = "LabelsToSave") List<String> nodeLabels, 
                                                    @Name(value = "Properties") String types, 
                                                    @Name (value = "Path") String path, 
                                                    @Name(value = "ZipFileName", defaultValue = "export") String zipFileName,
                                                    @Name(value = "Delimiter", defaultValue=";")String delimiter
    ) throws ProcedureException{
        try{
            Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
            NewExporter exporter = new NewExporter(neo4jAl, delimiter);
            Path output = exporter.exportTypeString(path, zipFileName,nodeLabels,types ); //---> change and make new method in exporter 
            return Stream.of(new OutputMessage(String.format("A new zip file has been created under '%s'.", output.toString())));
        } catch (Exception | FileIOException e) {
            log.error("Failed to export the list of nodes.", e);
            throw new ProcedureException("Failed to export the list of node. Check Neo4J logs for more details...", e);
        }

        
    }
    

     public SaverProcedure() { } // Neo4J POJO **/
}
