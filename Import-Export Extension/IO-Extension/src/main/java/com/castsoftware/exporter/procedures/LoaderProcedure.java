package com.castsoftware.exporter.procedures;

import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.io.Importer;
import com.castsoftware.exporter.io.NewImporter;
import com.castsoftware.exporter.io.NewImporterType;
import com.castsoftware.exporter.results.OutputMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

public class LoaderProcedure {
    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Transaction transaction;

    /**
     * Neo4 Procedure entry point for "fexporter.load()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.load(PathToZipFileName) - Import a configuration zip file to neo4j. \n" +
     "Parameters : \n" +
     "               - @PathToZipFileName - <String> - Location to saved output results. Ex : \"C:\\User\\John\\config.zip\"" +
     "Example of use : CALL fexporter.load(\"C:\\Neo4j_exports\\config.zip\")" +
     "") **/
     @Procedure(value = "fexporter.load", mode = Mode.WRITE)
     public Stream<OutputMessage> loadProcedure(@Name(value = "PathToZipFileName") String pathToZipFileName,
                                                @Name(value = "Delimiter", defaultValue=";") String delimiter
     ) throws ProcedureException {
         try {
            Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
            NewImporter importer = new NewImporter(neo4jAl, delimiter);
            importer.load(pathToZipFileName);

             return Stream.of(new OutputMessage("Zip file imported."));
         } catch (Exception e) {
            log.error("Failed to import the list of nodes.", e);
            throw new ProcedureException("Failed to import the list of node. Check Neo4J logs for more details...", e);
        }
     }

    /**
     * Neo4 Procedure entry point for "fexporter.load()". See Neo4j documentation for more information.
     * @throws ProcedureException

     @Description("fexporter.load(PathToZipFileName) - Import a configuration zip file to neo4j. \n" +
     "Parameters : \n" +
     "               - @PathToZipFileName - <String> - Location to saved output results. Ex : \"C:\\User\\John\\config.zip\"" +
     "Example of use : CALL fexporter.load(\"C:\\Neo4j_exports\\config.zip\")" +
     "") **/
     @Procedure(value = "fexporter.type.load", mode = Mode.WRITE)
     public Stream<OutputMessage> loadTypeProcedure(@Name(value = "PathToZipFileName") String pathToZipFileName,
                                                @Name(value = "Delimiter", defaultValue=";") String delimiter
     ) throws ProcedureException {
         try {
            Neo4jAl neo4jAl = new Neo4jAl(db, transaction, log);
            NewImporterType importer = new NewImporterType(neo4jAl, delimiter);
            importer.load(pathToZipFileName);

             return Stream.of(new OutputMessage("Zip file imported."));
         } catch (Exception e) {
            log.error("Failed to import the list of nodes.", e);
            throw new ProcedureException("Failed to import the list of node. Check Neo4J logs for more details...", e);
        }
     }


     
     

    /**
     * Neo4J Pojo
     */
    public LoaderProcedure() { }
}
