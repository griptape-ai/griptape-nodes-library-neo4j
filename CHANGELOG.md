# Changelog

All notable changes to the Griptape Nodes Neo4j Library will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/), and each version is
the `library_version` in `neo4j_nodes_library/griptape_nodes_library.json`.
**Breaking** marks a change that can stop a saved workflow from working without edits.

## [Unreleased]

<!-- Entries go in changelog.d/, one file each. See changelog.d/README.md. -->

## [0.42.0] - 2026-11-06

### Changed

- **Breaking:** the Neo4j Driver, Neo4j Connection, Execute Cypher, Create Node, Create Relationship,
  Find Nodes, Update Node and Delete Node nodes fail the run when something goes wrong. They used to
  show the error in the `Message` output and finish normally, so a workflow could continue after a
  failed query with empty results. Workflows that relied on that need to fix the failing query,
  connection or input, or stop depending on the empty outputs.
  [#6](https://github.com/griptape-ai/griptape-nodes-library-neo4j/issues/6)
- Neo4j failures show a short message in the editor, with the server's error code and message as
  separate fields. A Cypher syntax error links to the Cypher manual, and a rejected username or
  password links to the `NEO4J_USERNAME` and `NEO4J_PASSWORD` settings when the connection uses the
  library defaults. Input problems such as invalid JSON, an empty query or an unconfirmed delete now
  appear on the node when it runs instead of as a message when the workflow starts. The library
  requires engine 0.105.0 or later.
  [#6](https://github.com/griptape-ai/griptape-nodes-library-neo4j/issues/6)

[Unreleased]: https://github.com/griptape-ai/griptape-nodes-library-neo4j/compare/v0.42.0...HEAD
[0.42.0]: https://github.com/griptape-ai/griptape-nodes-library-neo4j/compare/a6e2e394fd23fe509b875a5000d39074b6d09864...v0.42.0
