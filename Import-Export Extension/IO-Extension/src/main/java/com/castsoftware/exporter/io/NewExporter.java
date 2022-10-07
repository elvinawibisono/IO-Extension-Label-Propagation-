package com.castsoftware.exporter.io;

import com.castsoftware.exporter.config.getConfigValues;
import com.castsoftware.exporter.csv.Formatter;
import com.castsoftware.exporter.csv.NodeRecord;
import com.castsoftware.exporter.csv.RelationshipRecord;
import com.castsoftware.exporter.database.Neo4jAl;
import com.castsoftware.exporter.database.Neo4jAlUtils;
import com.castsoftware.exporter.exceptions.file.FileIOException;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.utils.RelationshipsUtils;
import com.fasterxml.jackson.databind.annotation.JsonAppend.Prop;
import com.opencsv.*;

import javassist.expr.NewArray;

import org.eclipse.jetty.util.ArrayUtil;
import org.neo4j.graphdb.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class NewExporter {

	private static final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString(); // .csv
	private static final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString(); // relationship
	private static final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString(); // node
	private static final String NO_RELATIONSHIP_WEIGHT = getConfigValues.Property.NO_RELATIONSHIP_WEIGHT.toString();//NW
	private static final String NO_RELATIONSHIP = getConfigValues.Property.NO_RELATIONSHIP.toString(); //NULL
	


	private final Neo4jAl neo4jAl;
	private String delimiter;
	private Boolean considerNeighbors;

	/**
	 * Build th standard CSV Writer to parse the zip file
	 * @param out File Writer
	 * @return
	 */
	public CSVWriterBuilder getCSVWriterBuilder(FileWriter out) {
		return new CSVWriterBuilder(out)
				.withSeparator(CSVWriter.DEFAULT_SEPARATOR)
				.withQuoteChar(CSVWriter.DEFAULT_QUOTE_CHARACTER)
				.withEscapeChar(CSVWriter.DEFAULT_ESCAPE_CHARACTER)
				.withLineEnd(CSVWriter.DEFAULT_LINE_END);
	}


	/**
	 * Appends all the files created during this process to the target zip.
	 * Every file appended will be remove once added to the zip.
	 * @param targetName Name of the ZipFile
	 * @throws IOException
	 */
	private Path createZip(Path path, String targetName, List<Path> toZipFiles) throws FileIOException {
		// Create the zip file
		String filename = targetName.concat(".zip");
		Path filepath = path.resolve(filename);
		File f =  filepath.toFile();
		this.neo4jAl.info(String.format("Creating zip file at '%s'..", filepath.toString()));

		try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(f))) {

			for(Path toZipFilePath : toZipFiles) {
				File fileToZip = toZipFilePath.toFile();
				String nameToZip = toZipFilePath.getFileName().toString();

				try (FileInputStream fileStream = new FileInputStream(fileToZip)){
					ZipEntry e = new ZipEntry(nameToZip);
					zipOut.putNextEntry(e);

					byte[] bytes = new byte[1024];
					int length;
					while((length = fileStream.read(bytes)) >= 0) {
						zipOut.write(bytes, 0, length);
					}
				} catch (Exception e) {
					this.neo4jAl.error("An error occurred trying to zip file with name : ".concat(nameToZip), e);
				}

				if(!fileToZip.delete()) this.neo4jAl.error("Error trying to delete file with name : ".concat(nameToZip));
			}

			return filepath;

		} catch (IOException e) {
			this.neo4jAl.error("An error occurred trying create zip file with name : ".concat(targetName), e);
			throw new FileIOException("An error occurred trying create zip file with name.", e, "SAVExCZIP01");
		}
	}

	/**
	 * Export a specific label to a specified path and return the list of files ( node + relationships ) created
	 * @param label Labels to export
	 * @param prop
	 * @return The list of files created
	 */
	private Path exportByLabel(Path path, String label) throws Exception {
		neo4jAl.info(String.format("Start to export label : %s..", label));

		String filename = NODE_PREFIX.concat(label).concat(EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){


			// Get the headers
			List<String> headers = NodeRecord.getHeaders(neo4jAl, label);

			neo4jAl.info(String.format("headers: [%s]", String.join(",", headers) ));

			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = getCSVWriterBuilder(out);

			try (ICSVWriter printer = builder.build()) {

				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				Iterator<Node> iter = neo4jAl.findNodes(Label.label(label));

				Node x;
				int count = 0;
				String[] values;
				while (iter.hasNext()) {
					x = iter.next();
					values = Formatter.toString(NodeRecord.getNodeRecord(neo4jAl, x, headers));
					printer.writeNext(values);

					// Count
					count++;
					if(count % 200 == 0) neo4jAl.info(String.format("%d nodes exported...", count));
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of nodes with labels '%s'.", label, e));
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the label '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		}
	}


	/**
	 * [modification]
	 * Export a specific label to a specified path and return the file created
	 * The file created will have a 2D table, where the x and y axis is the name of 
	 * the node and the weight of the relationship between two nodes 
	 * @param label Labels to export
	 * @param prop
	 * @return The list of files created
	 */
	
	private Path exportByKeys(Path path, String label, String prop) throws Exception {
		neo4jAl.info(String.format("Start to export label : %s..", label));

		String filename = NODE_PREFIX.concat(label).concat(EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();
		String rels; 

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){
 
			// Get the headers
			List<String> headers = NodeRecord.getTypeHeaders(neo4jAl, label, prop);
			neo4jAl.info(String.format("headers: [%s]", String.join(",", headers) ));

			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = getCSVWriterBuilder(out);

			try (ICSVWriter printer = builder.build()) {

				//store the headers in a new ArrayList 
				List<String> row = new ArrayList<>(headers); 
				//add a blank cell 
				row.add(0," "); 
				//print the header with the empty cell 
				printer.writeNext(row.toArray(new String[0]));

				//create 
				List<String>newRow = new ArrayList<>(); 

				for (int it1 = 0 ; it1<headers.size(); it1++){

					newRow = new ArrayList<>(); 
					newRow.add(headers.get(it1)); 

					 
					for(int it2 = 0; it2<headers.size(); it2++){
	
						List<String> relationship = RelationshipsUtils.getRelationship(neo4jAl,label, headers.get(it1),label,headers.get(it2)); 
						rels =  String.join(",",relationship);

						if(rels.equals("NULL")){

							newRow.addAll(relationship); 
			
						}

						else if(rels.equals("EXIST")){

							newRow.addAll(RelationshipsUtils.getRelationshipWeight(neo4jAl,label,headers.get(it1),label,headers.get(it2)));

						}
						
					}

					printer.writeNext(newRow.toArray(new String[0]));

				}
				
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));

		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the label '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		}
	}
	

	/**
	 * Export a specific label to a specified path and return the list of files ( node + relationships ) created
	 * @param label Labels to export
	 * @return The list of files created
	 */
	private Path exportBy(Path path, String label) throws Exception {
		neo4jAl.info(String.format("Start to export label : %s..", label));

		String filename = NODE_PREFIX.concat(label).concat(EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){

			// Get the headers
			List<String> headers = NodeRecord.getHeaders(neo4jAl, label);

			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = getCSVWriterBuilder(out);

			try (ICSVWriter printer = builder.build()) {
				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				Iterator<Node> iter = neo4jAl.findNodes(Label.label(label));

				Node x;
				int count = 0;
				String[] values;
				while (iter.hasNext()) {
					x = iter.next();
					values = Formatter.toString(NodeRecord.getNodeRecord(neo4jAl, x, headers));
					printer.writeNext(values);

					// Count
					count++;
					if(count % 200 == 0) neo4jAl.info(String.format("%d nodes exported...", count));
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of nodes with labels '%s'.", label, e));
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the label '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export label '%s'. File Error.", label));
		}
	}

	/**
	 * Export a specific label to a specified path and return the list of files ( node + relationships ) created
	 * @param label Labels to export
	 * @return The list of files created
	 */
	private Path exportById(Path path, String label, List<Node> nodes) throws Exception {
		neo4jAl.info(String.format("Start to export label : %s..", label));

		String filename = String.format("%s%s%s", NODE_PREFIX, label, EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){

			// Get the headers
			List<String> headers = NodeRecord.getHeaders(neo4jAl, label);

			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = getCSVWriterBuilder(out);

			try (ICSVWriter printer = builder.build()) {
				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				int count = 0;
				String[] values;
				for(Node x : nodes) {
					values = Formatter.toString(NodeRecord.getNodeRecord(neo4jAl, x, headers));
					printer.writeNext(values);

					// Count
					count++;
					if(count % 200 == 0) neo4jAl.info(String.format("%d nodes exported...", count));
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export %d nodes by ID. File Error.", nodes.size()));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the label '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export %d nodes by ID. File Error.", nodes.size()));
		}
	}

	/**
	 * Export the relationships
	 * @param neo4jAl Neo4j Access Layer
	 * @param path Path of export folder
	 * @param type Type to export
	 * @return
	 */
	private Path exportRelationshipsByLabel(Neo4jAl neo4jAl, Path path, String type, String label) throws Exception {
		neo4jAl.info(String.format("Start to export relationship : %s..", type));

		String filename = RELATIONSHIP_PREFIX.concat(type).concat(EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){

			// Get the headers
			List<String> headers = RelationshipRecord.getHeaders(neo4jAl, type);

			char charDel = this.delimiter.isEmpty() ? CSVWriter.DEFAULT_SEPARATOR : this.delimiter.charAt(0);

			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = getCSVWriterBuilder(out);
			try (ICSVWriter printer = builder.build()) {

				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				Iterator<Node> iter = neo4jAl.findNodes(Label.label(label));
				int count = 0;
				Node x;
				String[] values;
				while (iter.hasNext()) {
					x = iter.next();
					for(Relationship rel : RelationshipsUtils.getRelationships(x, type, Direction.BOTH)) {
						values = Formatter.toString(RelationshipRecord.getRelationshipRecord(neo4jAl, rel, headers));
						printer.writeNext(values);

						// Count
						count++;
						if(count % 200 == 0) neo4jAl.info(String.format("%d relationships exported...", count));
					}
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		} catch (Neo4jQueryException e) {
			neo4jAl.error(String.format("Failed to get the list of nodes with labels '%s'.", label, e));
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the relationship '%s'.", type), e);
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		}
	}

	/**
	 * Export the relationships bu Id
	 * @param neo4jAl Neo4j Access Layer
	 * @param path Path of export folder
	 * @param type Type to export
	 * @return
	 */
	private Path exportRelationshipsByNodes(Neo4jAl neo4jAl, Path path, String type, List<Node> nodes) throws Exception {
		neo4jAl.info(String.format("Start to export relationship : %s..", type));

		String filename =  String.format("%s%s%s", RELATIONSHIP_PREFIX, type, EXTENSION);
		Path filepath = path.resolve(filename);
		File outputFile = filepath.toFile();

		// Open the file
		try (FileWriter out = new FileWriter(outputFile)){

			// Get the headers
			List<String> headers = RelationshipRecord.getHeaders(neo4jAl, type);

			char charDel = this.delimiter.isEmpty() ? CSVWriter.DEFAULT_SEPARATOR : this.delimiter.charAt(0);

			// Open the CSV printer - Build the configuration
			CSVWriterBuilder builder = getCSVWriterBuilder(out);
			try (ICSVWriter printer = builder.build()) {

				// Append header
				printer.writeNext(headers.toArray(new String[0]));

				// Parse nodes and append to the list
				int count = 0;
				String[] values;
				for (Node n : nodes) {
					for(Relationship rel : RelationshipsUtils.getRelationships(n, type, Direction.BOTH)) {
						values = Formatter.toString(RelationshipRecord.getRelationshipRecord(neo4jAl, rel, headers));
						printer.writeNext(values);

						// Count
						count++;
						if(count % 200 == 0) neo4jAl.info(String.format("%d relationships exported...", count));
					}
				}
			}

			return filepath;

		} catch (IOException e) {
			neo4jAl.error(String.format("Failed to create a file at '%s'.", filepath.toString()), e);
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		} catch (Exception e) {
			neo4jAl.error(String.format("Unexpected error during processing of the relationship '%s'.", type), e);
			throw new Exception(String.format("Failed to export relationship '%s'. File Error.", type));
		}
	}

	/**
	 * Export the list of relationships
	 * @param neo4jAl Neo4j Access Layer
	 * @param labels Labels to process
	 * @return
	 */
	private List<Path> exportLabelsRelationships(Neo4jAl neo4jAl, Path directory, List<String> labels) throws Exception {
		List<Path> paths = new ArrayList<>();
		Map<String, List<String>> headersMap = new HashMap<>();
		Path temp = null;
		// For each labels, export the related type of relationships
		for(String label : labels) {
			// Get the list
			List<String> relationshipTypes = RelationshipsUtils.getRelationshipTypeForLabel(neo4jAl, label);
			neo4jAl.info(String.format("%d relationships identified for label '%s'. Relationships: [ %s ]",
					relationshipTypes.size(), label, String.join(", ", relationshipTypes)));

			for(String type : relationshipTypes) {
				temp = this.exportRelationshipsByLabel(neo4jAl, directory, type, label);
				paths.add(temp);
			}
		}

		return paths;
	}

	

	/**
	 * Export the list of relationships
	 * @param neo4jAl Neo4j Access Layer
	 * @param nodes List of node to process
	 * @return
	 */
	private List<Path> exportNodesRelationships(Neo4jAl neo4jAl, Path directory, List<Node> nodes) throws Exception {
		List<Path> paths = new ArrayList<>();
		Map<String, List<String>> headersMap = new HashMap<>();
		Path temp = null;

		// For each labels, export the related type of relationships
		List<String> relationshipTypes = RelationshipsUtils.getRelationshipForNodes(neo4jAl, nodes);
		neo4jAl.info(String.format("%d relationship types identified for the %d nodes. Relationships: [ %s ]",
				relationshipTypes.size(), nodes.size(), String.join(", ", relationshipTypes)));

		for(String type : relationshipTypes) {
			temp = this.exportRelationshipsByNodes(neo4jAl, directory, type, nodes);
			paths.add(temp);
		}

		return paths;
	}

	/**
	 * Export the list of labels
	 * @param path Path to export
	 * @param fileName File name to export
	 */
	public Path exportLabelList(String path, String fileName, List<String> labels) throws Exception, FileIOException {
		neo4jAl.info("Verifying path and authorization...");
		Path directoryPath = Paths.get(path);
		if( !Files.exists(directoryPath)) throw new Exception(String.format("The specified directory doesn't exist. Path : '%s'.", path)); // If exists
		if(!Files.isWritable(directoryPath)) throw new Exception(String.format("Not enough authorization to write files: Path : '%s'.", path)); // If Writable
		neo4jAl.info("Path verified and writable");

		// For each label create, process and create files ( nodes + labels )
		neo4jAl.info("Exporting labels...");
		List<Path> createdFiles = new ArrayList<>();
		for(String label : labels) {
			createdFiles.add(this.exportByLabel(directoryPath, label));
		}
		neo4jAl.info("Labels exported.");

		// For each label, export the list of relationships
		neo4jAl.info("Exporting relationships...");
		createdFiles.addAll(this.exportLabelsRelationships(neo4jAl, directoryPath, labels));
		neo4jAl.info("Relationships exported.");

		// From the list of files created, zip them
		neo4jAl.info("Zipping the files created...");
		return this.createZip(directoryPath, fileName, createdFiles);
	}

	/**
	 * Export the list of labels
	 * @param path Path to export
	 * @param fileName File name to export
	 * @param ids List of long ids to export
	 */
	public Path exportIdListString(String path, String fileName, List<Long> ids) throws Exception, Neo4jQueryException, FileIOException {
		neo4jAl.info("Verifying path and authorization...");
		Path directoryPath = Paths.get(path);
		if( !Files.exists(directoryPath)) throw new Exception(String.format("The specified directory doesn't exist. Path : '%s'.", path)); // If exists
		if(!Files.isWritable(directoryPath)) throw new Exception(String.format("Not enough authorization to write files: Path : '%s'.", path)); // If Writable
		neo4jAl.info("Path verified and writable");

		// For each label create, process and create files ( nodes + labels )
		neo4jAl.info("Exporting Nodes...");

		// Find labels by ID and sort them  in a map
		ids = ids.stream().distinct().collect(Collectors.toList()); // Filter to get distinct values
		Map<String, List<Node>> labeledNodes = Neo4jAlUtils.sortNodesByLabel(neo4jAl, ids);

		// Create Files for each node to export sorted by label
		List<Path> createdFiles = new ArrayList<>();
		for(Map.Entry<String, List<Node>> label : labeledNodes.entrySet()) {
			createdFiles.add(this.exportById(directoryPath, label.getKey(), label.getValue()));
		}
		neo4jAl.info("Labels exported. %d files were created during the process.");

		// Get nodes by Ids
		List<Node> nodes = new ArrayList<>();
		Iterator<Node> itNodes = neo4jAl.findNodes(ids);
		while (itNodes.hasNext()) nodes.add(itNodes.next());

		// For each label, export the list of relationships
		neo4jAl.info("Exporting relationships...");
		createdFiles.addAll(this.exportNodesRelationships(neo4jAl, directoryPath, nodes));
		neo4jAl.info("Relationships exported.");

		// From the list of files created, zip them
		neo4jAl.info("Zipping the files created...");
		return this.createZip(directoryPath, fileName, createdFiles);
	}

	/**
	 * [modified]
	 * Export the list of labels 
	 * @param path Path to export
	 * @param fileName File name to export
	 */
	public Path exportTypeString(String path, String fileName, List<String> labels, String prop) throws Exception, FileIOException {
		neo4jAl.info("Verifying path and authorization...");
		Path directoryPath = Paths.get(path);
		if( !Files.exists(directoryPath)) throw new Exception(String.format("The specified directory doesn't exist. Path : '%s'.", path)); // If exists
		if(!Files.isWritable(directoryPath)) throw new Exception(String.format("Not enough authorization to write files: Path : '%s'.", path)); // If Writable
		neo4jAl.info("Path verified and writable");

		// For each label create, process and create files ( nodes + labels )
		neo4jAl.info("Exporting labels...");
		List<Path> createdFiles = new ArrayList<>();
		for(String label : labels) {

			createdFiles.add(this.exportByKeys(directoryPath, label, prop));
		}
		neo4jAl.info("Labels exported.");

		// From the list of files created, zip them
		neo4jAl.info("Zipping the files created...");
		return this.createZip(directoryPath, fileName, createdFiles);
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void setConsiderNeighbors(Boolean considerNeighbors) {
		this.considerNeighbors = considerNeighbors;
	}

	/**
	 * Exporter
	 * @param neo4jAl
	 */
	public NewExporter(Neo4jAl neo4jAl, String delimiter) {
		this.neo4jAl = neo4jAl;
		this.delimiter = delimiter;
	}
}
