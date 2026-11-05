- **Breaking:** the Neo4j Driver, Neo4j Connection, Execute Cypher, Create Node, Create Relationship,
  Find Nodes, Update Node and Delete Node nodes fail the run when something goes wrong. They used to
  show the error in the `Message` output and finish normally, so a workflow could continue after a
  failed query with empty results. Workflows that relied on that need to fix the failing query,
  connection or input, or stop depending on the empty outputs.
  [#6](https://github.com/griptape-ai/griptape-nodes-library-neo4j/issues/6)
