
CREATE
  (nAlice:User {name: 'Alice'}),
  (nBridget:User {name: 'Bridget'}),
  (nCharles:User {name: 'Charles'}),
  (nDoug:User {name: 'Doug'}),
  (nMark:User {name: 'Mark'}),
  (nMichael:User {name: 'Michael'}),

  (nBridget)-[:LINK ]->(nAlice),
  (nCharles)-[:LINK ]->(nAlice),
  (nBridget)-[:LINK ]->(nCharles),

  (nAlice)-[:LINK ]->(nDoug),

  (nMark)-[:LINK ]->(nDoug),
  (nMark)-[:LINK ]->(nMichael),
  (nMichael)-[:LINK]->(nMark);