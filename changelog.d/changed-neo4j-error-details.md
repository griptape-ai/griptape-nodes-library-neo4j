- Neo4j failures show a short message in the editor, with the server's error code and message as
  separate fields. A Cypher syntax error links to the Cypher manual, and a rejected username or
  password links to the `NEO4J_USERNAME` and `NEO4J_PASSWORD` settings when the connection uses the
  library defaults. Input problems such as invalid JSON, an empty query or an unconfirmed delete now
  appear on the node when it runs instead of as a message when the workflow starts. The library
  requires engine 0.105.0 or later.
  [#6](https://github.com/griptape-ai/griptape-nodes-library-neo4j/issues/6)
