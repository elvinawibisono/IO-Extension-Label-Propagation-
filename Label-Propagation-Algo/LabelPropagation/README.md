# Label Propagation
### Label Propgation algorithm where the graph value decreases as it iterate to nodes and become a community once it reaches 0 


** This label propagation works only to small tree graph with only one node label and one relationship label exist in the graph** 

## Installation

To install the Label Propagation, you can:

- First, build the Java project with Maven. Neo4j needs Java 8 to work properly, so make sure you have the correct JDK version. Once the build is complete, you should see a .jar package named 'friendly-neo4j-exporter-%VERSION%.jar' in the target repository. Drag & drop this file in the Neo4j plugin folder ( By default this folder is located in %NEO4J INSTALLATION FOLDER%\neo4j\plugins )


### How to call the procedure: 

# label propagtion of a graph with node label : User and relationship label: LINK
CALL LabelPropagation(Node_Label,Relationship) 

# Example of use : 
CALL fexporter.save("User","LINK")

#### With parameters :

- **@Node_Label** - *String* - Label to use, as a string. Ex : "User"
- **@Relationship** - *String* - Relationshop to use, as a string Ex : "LINK"; 

The output of this procedure would be creating new nodes under the label "Community". These nodes indicates as the community/communityId of the graph. You can see what nodes corresponds to the community/communityId nodes by clicking the community node and click the button that shows all the relationship of the node. 


## License
[GNU GPL](https://www.gnu.org/licenses/gpl-3.0.html)
