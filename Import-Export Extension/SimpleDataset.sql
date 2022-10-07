/* a simple dataset for Neo4j
* To use this dataset: Copy the whole code 
* from line 8 - 27 and run it to your local Neo4j 
*/



MERGE(Poland :Water {name: 'Poland ', weight: 5}) 
MERGE(Dasani:Water {name: 'Dasani', weight: 5}) 
MERGE(Arrowhead:Water {name: 'Arrowhead', weight: 5}) 
MERGE(SmartWater:Water {name: 'SmartWater', weight: 5}) 
MERGE(LifeWater:Water {name: 'LifeWater', weight:5}) 
Merge(Aquafina:Water {name: 'Aquafina', weight:5}) 
Merge(FIji:Water {name: 'FIji', weight:5})


MERGE (Poland )-[:Water {weight:1}]->(Dasani)
MERGE (Dasani)-[:Water {weight:1}]->(Poland )
MERGE (SmartWater )-[:Water]->(Arrowhead)
MERGE (Dasani)-[:Water {weight:1}]->(Arrowhead)
MERGE (Dasani)-[:Water {weight:1}]->(SmartWater)
MERGE (Dasani)-[:Water]->(LifeWater)
MERGE (Arrowhead)-[:Water {weight:1}]->(LifeWater)
MERGE (LifeWater)-[:Water ]->(Arrowhead)
MERGE (Aquafina)-[:Water {weight:1}]->(LifeWater)
MERGE (Aquafina)-[:Water ]->(FIji)
MERGE(FIji)-[:Water {weight:1}]->(Dasani)

