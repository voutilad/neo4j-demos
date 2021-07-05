// STEP 0 - cleanup
CALL gds.graph.list() YIELD graphName AS toBeDropped
WITH toBeDropped CALL gds.graph.drop(toBeDropped) YIELD graphName
RETURN graphName;

// STEP 0 : Bloom "Clients sharing identifiers"

//////////////////////////////////////////////////////////////////////////////
// Step 1
CALL db.labels() YIELD label
CALL apoc.cypher.run(
    'MATCH (:`'+label+'`) RETURN count(*) as cnt', {}) 
YIELD value

WITH label, value.cnt AS lblCount
CALL apoc.meta.stats() YIELD nodeCount
WITH *, toFloat(lblCount)/toFloat(nodeCount) AS pct
RETURN label AS `Node Label`,
    lblCount AS `Count`,
    round(pct * 100, 3) AS `%`
ORDER BY `%` DESC;



//////////////////////////////////////////////////////////////////////////////
// Step 2
// Get the total number of transactions in count, value, and frequency
MATCH (t:Transaction)
WITH sum(t.amount) AS globalSum, count(t) AS globalCnt
UNWIND ['CashIn', 'CashOut', 'Payment', 'Debit', 'Transfer'] AS txType
    CALL apoc.cypher.run('MATCH (t:' + txType + ') RETURN sum(t.amount) as txAmount, count(t) AS txCnt', {}) YIELD value
    RETURN txType,
        value.txAmount AS TotalMarketValue,
        100 * round(toFloat(value.txAmount) / toFloat(globalSum), 3) AS `%MarketValue`,
        100 * round(toFloat(value.txCnt) / toFloat(globalCnt), 3) AS `%MarketTransactions`,
        round(toFloat(value.txAmount) / toFloat(value.txCnt), 2) AS AvgTransactionValue,
        value.txCnt AS NumberOfTransactions
    ORDER BY `%MarketTransactions` DESC;


//////////////////////////////////////////////////////////////////////////////
// Step 3
// Create our projection called "wccGroups"
CALL gds.graph.create.estimate(
    ['Client', 'SSN', 'Email', 'Phone'],
    ['HAS_SSN', 'HAS_EMAIL', 'HAS_PHONE']);


//////////////////////////////////////////////////////////////////////////////
// Step 4
// Create our projection called "wccGroups"
CALL gds.graph.create('wccGroups',
    ['Client', 'SSN', 'Email', 'Phone'],
    ['HAS_SSN', 'HAS_EMAIL', 'HAS_PHONE']);



//////////////////////////////////////////////////////////////////////////////
// Step 5
// Call the WCC algorithm using our native graph projection
CALL gds.wcc.stream('wccGroups') YIELD nodeId, componentId

// Fetch the Node instance from the db and use its PaySim id
WITH componentId, collect(gds.util.asNode(nodeId).id) AS clients

// Identify groups where there are at least 2 clients
WITH *, size(clients) as groupSize WHERE groupSize > 1
RETURN * ORDER BY groupSize DESC LIMIT 1000;


//////////////////////////////////////////////////////////////////////////////
// Step 6
// Call the WCC algorithm using our native graph projection
CALL gds.wcc.stream('wccGroups') YIELD nodeId, componentId

// Fetch the Node instance from the db and use its PaySim id
WITH componentId, collect(gds.util.asNode(nodeId).id) AS clientIds
WITH *, size(clientIds) AS groupSize WHERE groupSize > 1

// Note that in this case, clients is a list of paysim ids.
// Let's unwind the list, MATCH, and tag them individually.
UNWIND clientIds AS clientId
    MATCH (c:Client {id:clientId})
    SET c.fraudGroup = componentId



//////////////////////////////////////////////////////////////////////////////
// Step 7
CREATE INDEX ON :Client(fraudGroup)


//// LOOK AT BLOOM: 1st Party Fraud Rings of size > 7

//////////////////////////////////////////////////////////////////////////////
// Step 8
// MATCH only our tagged Clients and group them by group size
MATCH (c:Client) WHERE c.fraudGroup IS NOT NULL
WITH c.fraudGroup AS groupId, collect(c.id) AS members
WITH groupId, size(members) AS groupSize
WITH collect(groupId) AS groupsOfSize, groupSize

RETURN groupSize,
        size(groupsOfSize) AS numOfGroups
ORDER BY groupSize DESC



//////////////////////////////////////////////////////////////////////////////
// Step 9
MATCH (c:Client) WHERE c.fraudGroup IS NOT NULL
WITH c.fraudGroup AS groupId, collect(c.id) AS members
WITH *, size(members) AS groupSize WHERE groupSize > 8

MATCH p=(c:Client {fraudGroup:groupId})-[:HAS_SSN|HAS_EMAIL|HAS_PHONE]->()
RETURN p


//////////////////////////////////////////////////////////////////////////////
// Step 10
// Recall our tagged Clients and group them by group size
MATCH (c:Client) WHERE c.fraudGroup IS NOT NULL
WITH c.fraudGroup AS groupId, collect(c.id) AS members
WITH groupId, size(members) AS groupSize WHERE groupSize > 8

// Expand our search to Clients one Transaction away
MATCH p=(:Client {fraudGroup:groupId})-[]-(:Transaction)-[]-(c:Client)
WHERE c.fraudGroup IS NULL
RETURN p



// Step 11
CALL gds.graph.create.cypher('rings',
    'MATCH (c:Client) WHERE c.fraudGroup IS NOT NULL RETURN id(c) AS id
     UNION
     MATCH (c:Client)-[]-(:Transaction)-[]-(c2:Client) WHERE c2.fraudGroup IS NOT NULL RETURN id(c) AS id
    ',
    'MATCH (c1:Client)-[]-(:Transaction)-[]-(c2:Client) WHERE c1.fraudGroup IS NOT NULL OR c2.fraudGroup IS NOT NULL RETURN id(c1) as source, id(c2) AS target'
)


// Step 12
call gds.wcc.stream('rings') yield nodeId, componentId
RETURN componentId, count(*)

// Step 13
CALL gds.graph.create.cypher('rings-directed',
    'MATCH (c:Client) WHERE c.fraudGroup IS NOT NULL RETURN id(c) AS id
     UNION
     MATCH (c:Client)-[]-(:Transaction)-[]-(c2:Client) 
       WHERE c2.fraudGroup IS NOT NULL RETURN id(c) AS id',

    'MATCH (c1:Client)-[]->(:Transaction)-[]->(c2:Client) 
     WHERE c1.fraudGroup IS NOT NULL OR c2.fraudGroup IS NOT NULL 
     RETURN id(c1) as source, id(c2) AS target'
)

// Step 14 - PageRank
CALL gds.pageRank.stream('rings-directed') YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC LIMIT 50

// Step 15 - PageRank
CALL gds.pageRank.write('rings-directed', {writeProperty: 'pageRank'})

//// LOOK AT BLOOM

// Step 16
MATCH (a)-[r:_ALL__]->(b) DELETE r;
MATCH (a)-[r:TRANSACTED_WITH]->(b) DELETE r;
CALL gds.graph.writeRelationship('rings-directed', '__ALL__');
MATCH (a)-[r:__ALL__]->(b)
DELETE r
CREATE (a)-[:TRANSACTED_WITH]->(b)


// LOOK AT BLOOM

// Step 17
CALL gds.betweenness.write('rings-directed', { writeProperty: 'betweenness' });
CALL gds.alpha.degree.write('rings-directed', {writeProperty: 'degree' });

// Step 18
MATCH (c:Client)
SET c.score = coalesce(c.betweenness, 0) / coalesce(c.degree, 1)

// LOOK AT BLOOM


//// FIN!