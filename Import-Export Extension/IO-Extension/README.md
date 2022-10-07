# Import-Export Extension for Neo4j
### Neo4j extension offering a better CSV export than the CSV exporter integrated in Neo4J

Extended Hugo's code for the import-export extension offering a CSV file export of graph's relationship weight. 

## Installation

To install the Friendly exporter, you have two options:

- First, build the Java project with Maven. Neo4j needs Java 8 to work properly, so make sure you have the correct JDK version. Once the build is complete, you should see a .jar package named 'friendly-neo4j-exporter-%VERSION%.jar' in the target repository. Drag & drop this file in the Neo4j plugin folder ( By default this folder is located in %NEO4J INSTALLATION FOLDER%\neo4j\plugins )

## Usage

### Export tree graph to a CSV file:

The complete procedure signature and options :
```python
# Save labels to CSV file format
CALL fexporter.save.types(LabelsToSave, Properties, Path, ZipFileName) 

# Example of use : 
CALL fexporter.save(["Water"], "name", "C:/Neo4j_exports/", "Result_05_09_20")
```

#### With parameters :

- **@LabelsToSave** - *String List* - Labels to save, as a list of string. Ex : ["C_relationship", "F_FrameworkRule"]
- **@Properties** - *String* - Specific property user want to extract. E.g. : name
- **@Path** - *String* - Location to save output results. Ex : "C:/User/John/Documents/"
- **@ZipFileName** - *String* - Name of the final zip file (the extension .zip will be automatically added). Ex : "Result_05_09_20" 

You can now explore the content of the zip, extract it, and zip it back for re-import.
The csv file output will be where x and y axis will be the name of the nodes and between the name of the nodes, will indicate the weight of the realtionship between two nodes 
if a relationship is present. (NULL -> no relationship is present between the two nodes , NW-> a relationship is present between two nodes but no weight)

### Import the CSV file back to Neo4j :

```python
# Import back into Neo4K
CALL fexporter.type.load("C:/exports/output.zip")

# Example of use : 
CALL fexporter.type.load("C:/Neo4j_exports/config.zip")
```

#### With parameters : 
- **@PathToZipFileName** - *String* - Location to saved output results (in zip format). Ex : "C:/Neo4j_exports/Result_05_09_20.zip"

This way your data will be re-import back into Neo4j.

## License
[GNU GPL](https://www.gnu.org/licenses/gpl-3.0.html)
